#ifndef sw_x0_Actor_h
#define sw_x0_Actor_h (1)

#include <deque>
#include <pthread.h>

namespace x0 {

template<typename Message>
class Actor
{
private:
	std::deque<Message> messages_;
	bool shutdown_;
	pthread_t thread_;
	pthread_mutex_t lock_;
	pthread_cond_t condition_;

public:
	Actor();
	virtual ~Actor();

	void send(Message message);
	void push_back(Message message) { send(message); }
	virtual void process(Message message) = 0;

	void start();
	void stop();
	void join();

	void main();
private:
	static void* _main(void* self);
};

// {{{ impl
template<typename Message>
inline Actor<Message>::Actor()
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
	pthread_create(&thread_, NULL, &Actor<Message>::_main, this);
}

template<typename Message>
inline void Actor<Message>::stop()
{
	shutdown_ = true;
	pthread_cond_signal(&condition_);
}

template<typename Message>
inline void Actor<Message>::join()
{
	void* tmp = nullptr;
	pthread_join(thread_, &tmp);
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

	while (!shutdown_) {
		pthread_cond_wait(&condition_, &lock_);

		while (!messages_.empty()) {
			Message message = messages_.front();
			messages_.pop_front();
			process(message);
		}
	}

	shutdown_ = false;
	pthread_mutex_unlock(&lock_);
}
// }}}

} // namespace x0

#endif
