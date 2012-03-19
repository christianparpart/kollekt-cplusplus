#ifndef sw_x0_Actor_h
#define sw_x0_Actor_h (1)

#include <deque>
#include <vector>
#include <pthread.h>

namespace x0 {

template<typename Message>
class Actor
{
private:
	bool shutdown_;
	std::deque<Message> messages_;
	std::vector<pthread_t> threads_;
	pthread_mutex_t lock_;
	pthread_cond_t condition_;

public:
	explicit Actor(size_t scalability = 1);
	virtual ~Actor();

	int scalability() const { return threads_.size(); }

	void send(Message message);
	void push_back(Message message) { send(message); }

	void start();
	void stop();
	void join();

protected:
	virtual void process(Message message) = 0;

private:
	void main();
	static void* _main(void* self);
};

// {{{ impl
template<typename Message>
inline Actor<Message>::Actor(size_t scalability) :
	shutdown_(false),
	messages_(),
	threads_(scalability),
	lock_(),
	condition_()
{
	pthread_mutex_init(&lock_, nullptr);
	pthread_cond_init(&condition_, nullptr);
}

template<typename Message>
inline Actor<Message>::~Actor()
{
	pthread_cond_destroy(&condition_);
	pthread_mutex_destroy(&lock_);
}

template<typename Message>
inline void Actor<Message>::send(Message message)
{
	pthread_mutex_lock(&lock_);
	messages_.push_back(message);
	pthread_mutex_unlock(&lock_);

	pthread_cond_signal(&condition_);
}

template<typename Message>
void Actor<Message>::start()
{
	shutdown_ = false;
	for (auto& thread: threads_)
		pthread_create(&thread, NULL, &Actor<Message>::_main, this);
}

template<typename Message>
inline void Actor<Message>::stop()
{
	shutdown_ = true;
	pthread_cond_broadcast(&condition_);
}

template<typename Message>
inline void Actor<Message>::join()
{
	for (auto thread: threads_) {
		void* tmp = nullptr;
		pthread_join(thread, &tmp);
	}
}

template<typename Message>
void* Actor<Message>::_main(void* selfp)
{
	Actor<Message>* self = reinterpret_cast<Actor<Message>*>(selfp);
	self->main();
	return self;
}

template<typename Message>
void Actor<Message>::main()
{
	pthread_mutex_lock(&lock_);

	for (;;) {
		pthread_cond_wait(&condition_, &lock_);

		if (shutdown_)
			break;

		Message message = messages_.front();
		messages_.pop_front();
		process(message);
	}

	pthread_mutex_unlock(&lock_);
}
// }}}

} // namespace x0

#endif
