#include <iostream>
#include <memory>
#include <unordered_map>
#include <atomic>
#include <list>
#include <vector>
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
#include <pthread.h>

std::vector<std::string> values = { // {{{
	"buonanotte", "uno", "quattro", "cinque", "sette", "dieci", "arrivederci", "arrivederla", "molto", "scusi", 
	"desidera?", "qualche", "parla", "lentamente", "acqua", "l'aranciata", "nostra", "auguri", "tutto", "come", 
	"questo", "questa", "l'ombra", "ombrello", "dove", "britannico", "americano", "australiano", "francese", 
	"diciannove", "venti", "forse", "Italia", "America", "Australia", "Francia", "l'uccello", "prego", "Canada", 
	"alzare", "gabinetto", "teatro", "costruzione", "tramonto", "salato", "amaro", "serpente", "mettere", 
	"sapere", "cavallo", "tiepido", "trattare", "campo", "cognome", "quadro", "cornice", "cadere", "fresco",   
	"certamente", "brutto", "grande", "piccolo", "consegnare", "l'anno", "Mario", "diciassette", "diciotto", 
	"lavoro", "anziano", "chiaro", "domandare", "suonare", "farmacia", "casalinga", "fiamma", "aeroporto", 
	"pescheria", "macelleria", "giardino", "largo", "stretto", "contrario", "rispondere", "azzurro", 
	"canadese", "Spagna", "spagnolo", "Portogallo", "arancione", "ventuno", "ventidue", "ventitre", "l'ora", 
	"l'appuntamento", "ovest", "nord", "sud-ovest", "ventisei", "ventisette", "ventotto", "ventinove", 
	"abitare", "abito", "tutto", "tutto", "lavorare", "lavorare", "l'incontro", "quanto?", "quanta", "pagare", 
	"tedesco", "corto", "lungo", "cattivo", "qualche", "undici", "dodici", "tredici", "quattordici", "quindici", 
	"platino", "ferro", "rocca", "tuono", "grandine", "ventoso", "colla", "prato", "parco", "dentista", "mappa", 
}; // }}}

class Producer // {{{
{
private:
	std::vector<std::string> keys_;
	pthread_t thread_;
	const char* host_;
	int port_;
	long long message_count_;

public:
	Producer(const char* host, int port, long long message_count) :
		keys_(),
		thread_(),
		host_(host),
		port_(port),
		message_count_(message_count)
	{
		for (unsigned i = 0; i < 64; ++i) {
			keys_.push_back(genkey());
		}
	}

	~Producer()
	{
	}

	void start()
	{
		pthread_create(&thread_, nullptr, &_run, this);
	}

	void join()
	{
		void* result = nullptr;
		pthread_join(thread_, &result);
	}

private:
	static void* _run(void* self)
	{
		reinterpret_cast<Producer*>(self)->run();
		return nullptr;
	}

	static std::string genkey()
	{
		static const char keymap[] = { "1234567890abcdef" };
		char buf[33];

		for (size_t i = 0; i < sizeof(buf) - 1; ++i)
			buf[i] = keymap[rand() % 16];

		buf[sizeof(buf) - 1] = '\0';
		return buf;
	}

	std::string getkey()
	{
		static size_t i = 0;
		return keys_[++i % keys_.size()];
		//std::string key = keys_[rand() % keys_.size()];
		//return key;
	}

	std::string getvalue()
	{
		return values[rand() % values.size()];
	}

	void run()
	{
		int fd_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
		if (fd_ < 0) {
			perror("socket");
			return;
		}

		struct sockaddr_in sin;
		memset(&sin, 0, sizeof(sin));
		sin.sin_family = AF_INET;
		sin.sin_port = htons(port_);

		int rv = inet_pton(AF_INET, host_, &sin.sin_addr.s_addr);
		if (rv == 0) {
			std::fprintf(stderr, "address [%s] not in representation format.\n", host_);
			return;
		} else if (rv < 0) {
			perror("inet_pton");
			return;
		}

		unsigned long long counter = 0;

		while (message_count_ != 0) {
			char buf[80];
			ssize_t buflen = snprintf(buf, sizeof(buf), "%s;%s", getkey().c_str(), getvalue().c_str());
			if (sendto(fd_, buf, buflen, 0, (sockaddr*)&sin, sizeof(sin)) < 0) {
				perror("sendto");
				break;
			}

			++counter;

			if (message_count_ > 0)
				--message_count_;

			if ((counter % 1024) == 0) {
				keys_[rand() % keys_.size()] = genkey();
			}
		}

		close(fd_);
	}
}; // }}}

int main(int argc, char* argv[])
{
	static const struct option long_options[] = {
		{ "help", no_argument, NULL, 'h' },
		{ "port", required_argument, NULL, 'p' },
		{ "address", required_argument, NULL, 'h' },
		{ "concurrency", required_argument, NULL, 'c' },
		{ "message-count", required_argument, NULL, 'n' },
		{ 0, 0, 0, 0 }
	};

	std::string address = "127.0.0.1";
	int port = 2323;
	int concurrency = 1; // number of producers (threads) in parallel
	long long message_count = -1; // number of messages per producer

	srandom(time(nullptr));

	for (bool args_parsed = false; !args_parsed; ) {
		int long_index = 0;
		switch (getopt_long(argc, argv, "?hp:h:c:n:", long_options, &long_index)) {
			case '?':
			case 'h':
				printf(
					"%s [-c NUM] [-n NUM] | [-h]\n"
					"\n"
					"  -h, --help               print this help\n"
					"  -a, --address=IP         sets the target host IP address [%s]\n"
					"  -p, --port=NUM           sets the target UDP port number [%d]\n"
					"  -c, --concurrency=NUM    number of concurrent threads [%d]\n"
					"  -n, --message-count=NUM  number of messages to sent per thread [%lld]\n"
					"                           (a value of -1 means unlimited)\n"
					"\n"
					"  This tool produces kollektor compatible message streams to performance-test\n"
					"  the kollektor implementation.\n"
					"\n",
					argv[0], address.c_str(), port, concurrency, message_count);
				return 0;
			case 'p':
				port = std::atoi(optarg);
				break;
			case 'a':
				address = optarg;
				break;
			case 'c':
				concurrency = std::atoi(optarg);
				break;
			case 'n':
				message_count = std::atoll(optarg);
				break;
			case 0:
				// long option with (val != NULL && flag == 0)
				break;
			case -1:
				// EOF - everything parsed
				args_parsed = true;
				break;
			default:
				return 1;
		}
	}

	std::printf("Spawning %d concurrent producers ...\n", concurrency);

	std::list<std::shared_ptr<Producer>> producers;

	for (int i = 0; i < concurrency; ++i)
		producers.push_back(std::make_shared<Producer>(address.c_str(), port, message_count));

	for (auto producer: producers)
		producer->start();

	for (auto producer: producers)
		producer->join();

	return 0;
}
