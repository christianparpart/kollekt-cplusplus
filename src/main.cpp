#include "PerformanceCounter.h"
#include <iostream>
#include <unordered_map>
#include <atomic>
#include <list>
#include <deque>
#include <string>
#include <sstream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <climits>
#include <ctime>
#include <getopt.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include <fcntl.h>
#include <ev++.h>

#include <pthread.h>

#if 0
#	define DEBUG(msg...) std::fprintf(stderr, msg)
#else
#	define DEBUG(msg...) /*!*/
#endif

class Server;

class Bucket // {{{
{
private:
	Server* server_;
	ev::loop_ref loop_;
	ev::timer idleTimer_;
	ev::timer ttlTimer_;
	std::string id_;
	int stream_[2];
	size_t streamSize_;
	size_t itemCount_;

	friend class Writer;

public:
	Bucket(Server* server, const char* id, size_t idsize);
	~Bucket();

	bool healthy() const { return stream_[0] >= 0; }

	const std::string& id() const { return id_; }
	void push_back(const char* value, size_t size);

private:
	void flush();
	void timeoutTTL(ev::timer&, int);
	void timeoutIdle(ev::timer&, int);
}; // }}}

class Writer // {{{
{
private:
	ev::loop_ref loop_;
	std::string storagePath_;
	int currentChunkId_; // the current (e.g.) hour. re-open the output file once this unit differs to the current (e.g.) hour
	size_t outputOffset_;
	int fd_; // handle to the current open output file
	std::deque<Bucket*> buckets_;
	pthread_t thread_;
	bool shutdown_;

	pthread_mutex_t bucketsLock_;
	pthread_mutex_t waitLock_;
	pthread_cond_t condition_;

public:
	explicit Writer(ev::loop_ref loop);
	~Writer();

	void setStoragePath(const std::string& path) { storagePath_ = path; }
	void start();
	void stop();
	void join();
	void push_back(Bucket* bucket);

private:
	static void* _run(void* self);
	void main();
	bool checkOutput();
	void writeBuckets();
}; // }}}

class Server // {{{
{
private:
	ev::loop_ref loop_;
	int fd_;
	ev::io io_;
	ev::sig usr1Signal_;
	ev::sig termSignal_;
	ev::sig intSignal_;
	std::unordered_map<std::string, Bucket*> buckets_;
	Writer writer_;
	x0::PerformanceCounter<8, size_t> bytesRead_;
	x0::PerformanceCounter<8, size_t> bytesProcessed_;
	x0::PerformanceCounter<8, size_t> messagesProcessed_;

	// resource limits
	size_t maxBucketCount_;
	size_t maxBucketSize_;
	size_t maxBucketIdle_;
	size_t maxBucketTTL_;

	std::atomic<size_t> bucketCount_;

	// statistical
	std::atomic<size_t> bucketsKilledMaxSize_;
	std::atomic<size_t> bucketsKilledMaxAge_;
	std::atomic<size_t> bucketsKilledMaxIdle_;
	std::atomic<size_t> bucketsKilledSysError_;
	std::atomic<size_t> droppedMessages_;

	friend class Bucket;

public:
	explicit Server(ev::loop_ref ev);
	~Server();

	bool setup(int argc, char* argv[]);
	void flush(Bucket* bucket);
	void join() { writer_.join(); }

private:
	bool start(int port, const char* address = "0.0.0.0");
	void stop();
	void printHelp(const char* program);
	void incoming(ev::io& io, int revents);
	void sigterm(ev::sig& sig, int revents);
	void logStats(ev::sig& sig, int revents);
}; // }}}

// {{{ Bucket impl
Bucket::Bucket(Server* server, const char* id, size_t idsize) :
	server_(server),
	loop_(server->loop_),
	idleTimer_(server->loop_),
	ttlTimer_(server->loop_),
	id_(id, 0, idsize),
	stream_(),
	streamSize_(0),
	itemCount_(0)
{
	++server_->bucketCount_;
	DEBUG("Bucket[%s].new (count=%lu)\n", id_.c_str(), server_->bucketCount_.load());
	if (pipe(stream_) < 0) {
		// pipe creation failed
		stream_[0] = stream_[1] = -1;
		perror("pipe");
		// TODO error checking inside its caller
	} else {
		char buf[64];
		ssize_t buflen = snprintf(buf, sizeof(buf), "\n%f;", ev_now(loop_));
		::write(stream_[1], buf, buflen);
		::write(stream_[1], id, idsize);
		streamSize_ += buflen + idsize;

		idleTimer_.set<Bucket, &Bucket::timeoutIdle>(this);
		idleTimer_.start(server_->maxBucketIdle_, 0.0);

		ttlTimer_.set<Bucket, &Bucket::timeoutTTL>(this);
		ttlTimer_.start(server_->maxBucketTTL_, 0.0);
	}
}

Bucket::~Bucket()
{
	DEBUG("Bucket[%s].destroy\n", id_.c_str());

	if (stream_[0] >= 0) {
		::close(stream_[0]);
		::close(stream_[1]);
	}

	--server_->bucketCount_;
}

// value passed including the leading ';'
void Bucket::push_back(const char* value, size_t size)
{
	DEBUG("Bucket[%s] << '%s'\n", id_.c_str(), value);
	ssize_t rv = ::write(stream_[1], value, size);

	if (rv < 0) {
		perror("write");
		++server_->bucketsKilledSysError_;
		flush();
		return;
	}

	streamSize_ += size;
	++itemCount_;

	if (itemCount_ == server_->maxBucketSize_) {
		++server_->bucketsKilledMaxSize_;
		flush();
		return;
	}

	if (idleTimer_.is_active())
		idleTimer_.stop();

	idleTimer_.start(server_->maxBucketIdle_, 0.0);
}

void Bucket::flush()
{
	if (idleTimer_.is_active())
		idleTimer_.stop();

	if (ttlTimer_.is_active())
		ttlTimer_.stop();

	server_->flush(this);
}

void Bucket::timeoutTTL(ev::timer&, int)
{
	DEBUG("Bucket[%s].timeoutTTL()\n", id_.c_str());
	++server_->bucketsKilledMaxAge_;
	flush();
}

void Bucket::timeoutIdle(ev::timer&, int)
{
	DEBUG("Bucket[%s].timeoutIdle()\n", id_.c_str());
	++server_->bucketsKilledMaxIdle_;
	flush();
}
// }}}

// {{{ Writer impl
Writer::Writer(ev::loop_ref loop) :
	loop_(loop),
	storagePath_("."),
	currentChunkId_(0),
	outputOffset_(1),
	fd_(-1),
	buckets_(),
	thread_(),
	shutdown_(false),
	bucketsLock_(),
	waitLock_(),
	condition_()
{
	pthread_mutex_init(&bucketsLock_, nullptr);
	pthread_mutex_init(&waitLock_, nullptr);
	pthread_cond_init(&condition_, nullptr);
}

Writer::~Writer()
{
	pthread_cond_destroy(&condition_);
	pthread_mutex_destroy(&waitLock_);
	pthread_mutex_destroy(&bucketsLock_);
}

void Writer::start()
{
	pthread_create(&thread_, NULL, &Writer::_run, this);
}

void Writer::stop()
{
	shutdown_ = true;
	pthread_cond_signal(&condition_);
}

void Writer::join()
{
	void* result = nullptr;
	pthread_join(thread_, &result);
}

void* Writer::_run(void* p)
{
	reinterpret_cast<Writer*>(p)->main();
	return NULL;
}

void Writer::push_back(Bucket* bucket)
{
	DEBUG("Writer.push_back(): %s\n", bucket->id().c_str());
	pthread_mutex_lock(&waitLock_);
	buckets_.push_back(bucket);
	pthread_mutex_unlock(&waitLock_);

	pthread_cond_signal(&condition_);
}

void Writer::main()
{
	pthread_mutex_lock(&waitLock_);

	while (!shutdown_) {
		pthread_cond_wait(&condition_, &waitLock_);

		if (checkOutput())
			writeBuckets();
	}

	pthread_mutex_unlock(&waitLock_);
}

bool Writer::checkOutput()
{
	time_t now = std::time(nullptr);
	int chunkId = static_cast<time_t>(now) / (60 * 60);

	if (fd_ < 0 || chunkId != currentChunkId_) {
		if (fd_ >= 0)
			::close(fd_);

		char filename[PATH_MAX];
		snprintf(filename, sizeof(filename), "%s/%d.csv", storagePath_.c_str(), chunkId);

		fd_ = ::open(filename, O_WRONLY | O_CREAT, 0664);
		if (fd_ < 0) {
			std::fprintf(stderr, "Could not open log chunk file for writing: %s: %s\n", filename, strerror(errno));
			return false;
		}
		currentChunkId_ = chunkId;

		// manually seek to the end of the file (may not use O_APPEND due to splice()-requirements)
		ssize_t rv = lseek(fd_, 0, SEEK_END);
		if (rv >= 0)
			outputOffset_ = rv;

		// write CSV header-line
		static const char* header = "first_seen;key;values";
		rv = ::write(fd_, header, strlen(header));
		if (rv > 0)
			outputOffset_ += rv;

		DEBUG("Writer.checkOutput: opened file and start watching (fd=%d)\n", fd_);
	}

	return true;
}

void Writer::writeBuckets()
{
	DEBUG("Writer.writeBuckets()\n");

	while (!buckets_.empty()) {
		Bucket* bucket = buckets_.front();
		ssize_t rv = splice(
			bucket->stream_[0], NULL,
			fd_, NULL,
			bucket->streamSize_,
			SPLICE_F_MOVE | SPLICE_F_MORE
		);
		switch (rv) {
		case -1:
			perror("splice");
		case 0:
			std::fprintf(stderr, "splice() failed.\n");
			buckets_.pop_front();
			delete bucket;
			return;
		}
		bucket->streamSize_ -= rv;
		outputOffset_ += rv;
		DEBUG("- wrote bucket chunk of size %zi\n", rv);

		if (bucket->streamSize_ == 0) { // bucket fully flushed.
			DEBUG("- bucket fully flushed\n");
			buckets_.pop_front();
			delete bucket;
		}
	}

	DEBUG("Writer.writeBuckets: done\n");
}
// }}}

// {{{ Server impl
Server::Server(ev::loop_ref loop) :
	loop_(loop),
	io_(loop),
	usr1Signal_(loop),
	termSignal_(loop),
	intSignal_(loop),
	writer_(loop),
	bytesRead_(),
	bytesProcessed_(),
	messagesProcessed_(),
	maxBucketCount_((1024 - 7) / 2),
	maxBucketSize_(50),
	maxBucketIdle_(10),
	maxBucketTTL_(60),
	bucketCount_(0),
	bucketsKilledMaxSize_(0),
	bucketsKilledMaxAge_(0),
	bucketsKilledMaxIdle_(0),
	bucketsKilledSysError_(0),
	droppedMessages_(0)
{
	usr1Signal_.set<Server, &Server::logStats>(this);
	usr1Signal_.start(SIGUSR1);
	loop_.unref();

	termSignal_.set<Server, &Server::sigterm>(this);
	termSignal_.start(SIGTERM);
	loop_.unref();

	intSignal_.set<Server, &Server::sigterm>(this);
	intSignal_.start(SIGINT);
	loop_.unref();
}

Server::~Server()
{
	if (intSignal_.is_active()) {
		loop_.ref();
		intSignal_.stop();
	}

	if (termSignal_.is_active()) {
		loop_.ref();
		termSignal_.stop();
	}

	if (usr1Signal_.is_active()) {
		loop_.ref();
		usr1Signal_.stop();
	}

	if (fd_ >= 0) {
		stop();
	}
}

bool Server::setup(int argc, char* argv[])
{
	int port = 2323;
	char address[64] = "0.0.0.0";

	static const struct option long_options[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "port", required_argument, NULL, 'p' },
		{ "address", required_argument, NULL, 'a' },
		{ "storage-path", required_argument, NULL, 's' },
		{ "max-bucket-count", required_argument, NULL, 'c' },
		{ "max-bucket-size", required_argument, NULL, 'n' },
		{ "max-bucket-idle", required_argument, NULL, 'i' },
		{ "max-bucket-ttl", required_argument, NULL, 't' },
		{ 0, 0, 0, 0 }
	};

	for (;;) {
		int long_index = 0;
		switch (getopt_long(argc, argv, "?hp:a:s:c:n:i:t:", long_options, &long_index)) {
			case '?':
			case 'h':
				printHelp(argv[0]);
				return false;
			case 'p':
				port = std::atoi(optarg);
				break;
			case 'a':
				strncpy(address, optarg, sizeof(address));
				break;
			case 's':
				writer_.setStoragePath(optarg);
				break;
			case 'c':
				maxBucketCount_ = atoi(optarg);
				break;
			case 'n':
				maxBucketSize_ = atoi(optarg);
				break;
			case 'i':
				maxBucketIdle_ = atoi(optarg);
				break;
			case 't':
				maxBucketTTL_ = atoi(optarg);
				break;
			case 0:
				// long option with (val != NULL && flag == 0)
				break;
			case -1:
				// EOF - everything parsed
				return start(port, address);
				break;
			default:
				return false;
		}
	}
}

void Server::flush(Bucket* bucket)
{
	auto i = buckets_.find(bucket->id());
	if (i != buckets_.end()) {
		buckets_.erase(i);
		writer_.push_back(bucket);
	} else {
		std::fprintf(stderr, "Requested a flush of a bucket that is not (anymore) in the server's bucket set.\n");
		writer_.push_back(bucket);
	}
}

bool Server::start(int port, const char* address)
{
	// verify file descriptor limit
	size_t required_fd_count = 7 + maxBucketCount_ * 2;
	rlimit rlim;
	rlim.rlim_cur = rlim.rlim_max = required_fd_count;

	if (setrlimit(RLIMIT_NOFILE, &rlim) < 0) {
		perror("setrlimit");
	}

	if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
		if (required_fd_count > rlim.rlim_cur) {
			size_t adjusted_value = (rlim.rlim_cur - 7) / 2;
			std::fprintf(stderr,
				"Not enough file descriptors available to this process (%ld). "
				"Would require %ld file descriptors for %ld buckets. Adjusting maximum bucket count to %ld.\n",
				rlim.rlim_cur, required_fd_count, maxBucketCount_, adjusted_value);
			maxBucketCount_ = adjusted_value;
		}
	}

	fd_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (fd_ < 0) {
		perror("socket");
		return false;
	}

	struct sockaddr_in sin;
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port);

	int rv = inet_pton(AF_INET, address, &sin.sin_addr.s_addr);
	if (rv == 0) {
		std::cerr << "Listener address [" << address << "] not in representation format." << std::endl;
		return false;
	} else if (rv < 0) {
		perror("inet_pton");
		return false;
	}

	if (bind(fd_, (sockaddr*)&sin, sizeof(sin)) < 0) {
		perror("bind");
		return false;
	}

	io_.set(fd_, ev::READ);
	io_.set<Server, &Server::incoming>(this);
	io_.start();

	writer_.start();

	return true;
}

void Server::incoming(ev::io& io, int)
{
	struct sockaddr_in sin;
	socklen_t slen = sizeof(sin);
	char buf[4096];

	int rv = recvfrom(io.fd, buf, sizeof(buf), 0, (sockaddr*)&sin, &slen);
	if (rv > 0) {
		buf[rv] = '\0';

		time_t now = ev_now(loop_);
		bytesRead_.update(now, rv);

		if (bucketCount_ + 1 == maxBucketCount_) {
			++droppedMessages_;
			return;
		}

		bytesProcessed_.update(now, rv);

		if (char *p = strchr(buf, ';')) {
			size_t keysize = p - buf;
			size_t valsize = rv - keysize;

			*p = '\0';
			auto i = buckets_.find(buf);
			*p = ';';

			if (i != buckets_.end()) {
				// bucket found -> append value to existing bucket
				i->second->push_back(p, valsize);
				messagesProcessed_.update(now, 1);
			} else {
				// bucket doesn't exist yet -> create new bucket and push value into it
				Bucket* bucket = new Bucket(this, buf, keysize);
				if (bucket->healthy()) {
					buckets_[bucket->id()] = bucket;
					bucket->push_back(p, valsize);
					messagesProcessed_.update(now, 1);
				} else {
					delete bucket;
				}
			}
		}
	}
}

void Server::sigterm(ev::sig&, int)
{
	std::printf("Shutting down\n");
	stop();

	if (intSignal_.is_active()) {
		loop_.ref();
		intSignal_.stop();
	}

	if (termSignal_.is_active()) {
		loop_.ref();
		termSignal_.stop();
	}
}

void Server::logStats(ev::sig&, int)
{
	time_t now = ev_now(loop_);
	bytesRead_.update(now, 0);
	bytesProcessed_.update(now, 0);
	messagesProcessed_.update(now, 0);

	std::printf(
		"dropped: %ld, active: %ld, k/idle: %ld, k/ttl: %ld, k/size: %ld, k/syserr: %ld, "
		"bt/s: %.2f, bp/s: %.2f, m/s: %lu\n",
		droppedMessages_.load(),
		bucketCount_.load(),
		bucketsKilledMaxIdle_.load(),
		bucketsKilledMaxAge_.load(),
		bucketsKilledMaxSize_.load(),
		bucketsKilledSysError_.load(),
		bytesRead_.average() / (1024.0f * 1024.0f / 8.0f),
		bytesProcessed_.average() / (1024.0f * 1024.0f / 8.0f),
		messagesProcessed_.average()
	);
}

void Server::stop()
{
	io_.stop();
	::close(fd_);
	fd_ = -1;

	writer_.stop();
}

void Server::printHelp(const char* program)
{
	printf("usage: %s [-a ADDRESS] [-p PORT] [-s STORAGE_PATH] [resource options] | -h\n"
		   "\n"
		   "  -h, -?, --help               print this help\n"
		   "  -a, --address=ADDR           binds to this UDP address for listening\n"
		   "  -p, --port=PORT              sets the UDP listen port\n"
		   "  -s, --storage-path=PATH      set the logging output directory\n"
		   "  -c, --max-bucket-count=VALUE sets the limit of concurrently managed buckets\n"
		   "  -n, --max-bucket-size=VALUE  sets the limit of items per bucket\n"
		   "  -i, --max-bucket-idle=VALUE  sets the maximum bucket idle time in seconds\n"
		   "  -t, --max-bucket-ttl=VALUE   sets the bucket TTL (time to life) in seconds\n"
		   "\n",
		   program);
}
// }}}

int main(int argc, char* argv[])
{
	ev::default_loop loop;
	Server server(loop);

	if (!server.setup(argc, argv))
		return 1;

	ev_loop(loop, 0);
	server.join();

	std::printf("Exiting.\n");
	return 0;
}
