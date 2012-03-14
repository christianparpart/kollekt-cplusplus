#include <iostream>
#include <map>
#include <list>
#include <deque>
#include <string>
#include <sstream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <climits>
#include <getopt.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <ev++.h>

static const ev::tstamp BUCKET_TTL = 2 * 60; //!60 * 60; // 60 minutes
static const ev::tstamp BUCKET_IDLE_TIMEOUT = 30; //!10 * 60; // 10 minutes
static const size_t BUCKET_ELEMENT_LIMIT = 50;
static const char* BUCKET_STORAGE_BASEDIR = "."; // "/var/lib/kollekt";

class Server;

class Bucket // {{{
{
private:
	Server* server_;
	ev::loop_ref loop_;
	ev::timer idleTimer_;
	ev::timer ttlTimer_;
	std::string id_;
	std::stringstream stream_;

	friend class Writer;

public:
	Bucket(Server* server, const char* id);
	~Bucket();

	const std::string& id() const { return id_; }
	Bucket& operator<<(const char* value);

private:
	void timeout(ev::timer&, int);
}; // }}}

class Writer // {{{
{
private:
	ev::loop_ref loop_;
	std::string storagePath_;
	int currentUnit_; // the current (e.g.) hour. re-open the output file once this unit differs to the current (e.g.) hour
	int fd_; // handle to the current open output file

public:
	explicit Writer(ev::loop_ref loop);
	~Writer();

	void setStoragePath(const std::string& path) { storagePath_ = path; }

	Writer& operator<<(Bucket* bucket);
}; // }}}

class Server // {{{
{
private:
	ev::loop_ref loop_;
	int fd_;
	ev::io io_;
	std::map<std::string, Bucket*> buckets_;
	Writer writer_;

	friend class Bucket;

public:
	explicit Server(ev::loop_ref ev);
	~Server();

	bool setup(int argc, char* argv[]);
	void flush(Bucket* bucket);

private:
	bool start(int port, const char* address = "0.0.0.0");
	void stop();
	void printHelp(const char* program);
	void incoming(ev::io& io, int revents);
}; // }}}

// {{{ Bucket impl
Bucket::Bucket(Server* server, const char* id) :
	server_(server),
	loop_(server->loop_),
	idleTimer_(server->loop_),
	ttlTimer_(server->loop_),
	id_(id),
	stream_()
{
	stream_ << id;

	idleTimer_.set<Bucket, &Bucket::timeout>(this);
	idleTimer_.start(BUCKET_IDLE_TIMEOUT, 0.0);

	ttlTimer_.set<Bucket, &Bucket::timeout>(this);
	ttlTimer_.start(BUCKET_TTL, 0.0);
}

Bucket::~Bucket()
{
}

Bucket& Bucket::operator<<(const char* value)
{
	//std::printf("Bucket[%s] << '%s'\n", id_.c_str(), value);
	stream_ << ';' << value;

	if (idleTimer_.is_active())
		idleTimer_.stop();

	idleTimer_.start(BUCKET_IDLE_TIMEOUT, 0.0);

	return *this;
}

void Bucket::timeout(ev::timer&, int)
{
	//std::printf("Bucket[%p].timeout()\n", this);
	server_->flush(this);
}
// }}}

// {{{ Writer impl
Writer::Writer(ev::loop_ref loop) :
	loop_(loop),
	currentUnit_(0),
	fd_(-1)
{
}

Writer::~Writer()
{
}

Writer& Writer::operator<<(Bucket* bucket)
{
	//std::printf("Writer << %s\n", bucket->id().c_str());

	char filename[PATH_MAX];
	ev::tstamp now = ev_now(loop_);
	snprintf(filename, sizeof(filename), "%s/%lu.csv", BUCKET_STORAGE_BASEDIR,
		static_cast<time_t>(now) / (60 * 60));

	if (FILE* fp = fopen(filename, "a")) {
		fprintf(fp, "%f;%s\n", ev_now(loop_), bucket->stream_.str().c_str());
		fclose(fp);
	}
	delete bucket;
	return *this;
}
// }}}

// {{{ Server impl
Server::Server(ev::loop_ref loop) :
	loop_(loop),
	io_(loop),
	writer_(loop)
{
}

Server::~Server()
{
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
		{ "port", required_argument, &port, 'p' },
		{ "address", required_argument, NULL, 'a' },
		{ "storage-path", required_argument, NULL, 's' },
		{ 0, 0, 0, 0 }
	};

	for (;;) {
		int long_index = 0;
		switch (getopt_long(argc, argv, "?hp:a:s:", long_options, &long_index)) {
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
	buckets_.erase(buckets_.find(bucket->id()));
	writer_ << bucket;
}

bool Server::start(int port, const char* address)
{
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
		if (char *p = strchr(buf, ';')) {
			*p = '\0';
			++p;
			auto i = buckets_.find(buf);
			if (i != buckets_.end()) {
				// bucket found -> append value to existing bucket
				(*i->second) << p;
			} else {
				// bucket doesn't exist yet -> create new bucket and push value into it
				Bucket* bucket = new Bucket(this, buf);
				*bucket << p;
				buckets_[buf] = bucket;
			}
		}
	}
}

void Server::stop()
{
	io_.stop();
	::close(fd_);
	fd_ = -1;
}

void Server::printHelp(const char* program)
{
	printf("usage: %s [-a ADDRESS] [-p PORT] [-s STORAGE_PATH] | -h\n", program);
}
// }}}

int main(int argc, char* argv[])
{
	ev::default_loop loop;

	Server server(loop);

	if (!server.setup(argc, argv))
		return 1;

	ev_loop(loop, 0);
	return 0;
}
