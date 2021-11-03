#include <iostream>
#include "uthreads.h"
#include <setjmp.h>
#include <signal.h>
#include <unistd.h>
//#include <ctime>
#include <deque>
#include <map>
#include <sys/time.h>
#include <set>
//--------------------MACROS--------------------------------------
#define MAIN_THREAD 0
#define SUCCESS 0
#define DEFAULT_MUTEX -1
#define FAILED_EXIT 1
#define STATE_READY "READY"
#define STATE_RUNNING "RUNNING"
#define STATE_BLOCKED "BLOCKED"
#define FAIL -1
#define SUCCESS_INIT 0
#define CONVERT_SECOND_TO_MICRO 1000000
#define NON_POSITIVE_QUANT 0
//---------------------Error messages-----------------------------
#define SYSTEM_CALL_FAIL "system error: "
#define LIBRARY_FAIL "thread library error: "
#define SIGEMPTY_ERROR "sigemptyset failed"
#define SIGADD_ERROR "sigaddset failed"
#define BAD_INPUT_NUM "quantom should be non negative int. number"
#define SIGACT_ERROR "sigaction failed"
#define SETIME_ERROR "setitimer failed"
#define SIGPRO_ERROR "sigprocmask failed"
#define MAX_THREADS_ERROR "reached max num of threads"
#define MUTEX_NOTLOCKED_ERROR "mutex is not locked"
#define MUTEX_ERR "mutex is locked by different thread"
#define MUTEX_LOCKED_ERROR "mutex locked by this thread"
#define BLOCK_MAIN "can't block main thread"
#define NO_THREAD "thread does not exist"
#define BAD_POINTER "function pointer is wrong"

using namespace std;

#ifdef __x86_64__
/* code for 64 bit Intel arch */
typedef unsigned long address_t;
#define JB_SP 6
#define JB_PC 7
/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
  address_t ret;
  asm volatile("xor    %%fs:0x30,%0\n"
               "rol    $0x11,%0\n"
  : "=g" (ret)
  : "0" (addr));
  return ret;
}
#else
/* code for 32 bit Intel arch */

typedef unsigned int address_t;
#define JB_SP 4
#define JB_PC 5

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
    address_t ret;
    asm volatile("xor    %%gs:0x18,%0\n"
		"rol    $0x9,%0\n"
                 : "=g" (ret)
                 : "0" (addr));
    return ret;
}
#endif

/*describes a mutex lock*/
struct mutex{
  int taken_by_id=DEFAULT_MUTEX;
  bool is_locked=false;
}typedef mutex;

/* Class describes a User Thread */
class Uthread
{
 public:
  Uthread(unsigned int input_id,void (*f)(void)):thread_id(0),stack(new char[STACK_SIZE]),
  thread_state
      (STATE_READY),running_quantom_times(0){
    if(input_id!=0){
      address_t sp, pc;

      sp = (address_t)stack + STACK_SIZE - sizeof(address_t);
      pc = (address_t)f;
      sigsetjmp(env, 1);
      (env->__jmpbuf)[JB_SP] = translate_address(sp);
      (env->__jmpbuf)[JB_PC] = translate_address(pc);
    }
    else{
      running_quantom_times++;
    }
    if(sigemptyset(&env->__saved_mask)){
      cerr<<SYSTEM_CALL_FAIL<< SIGEMPTY_ERROR<< endl;
    }
  }
  ~Uthread(){
    delete[] stack; //delete stack allocated for thread
    stack = nullptr;
  }
  /*-----------------------------data members--------------------------*/
  unsigned int thread_id;
  char * stack;
  string thread_state;
  unsigned int running_quantom_times;
  sigjmp_buf env{}; //for saving thread state
  bool blocked_by_mutex=false;
  bool blocked_by_func = false; //blocked by "block_thread"
  bool wants_mutex =false;

};

//--------------------Global Variables---------------------------------
deque<int> ready_queue;
set<Uthread*> blocked_threads;
map<int,Uthread*> user_threads;
sigset_t signals_set ={0};
int library_quantum=0;
struct sigaction sa = {};
struct itimerval timer;
int current_running_thread = -1;
mutex mutex_lock;
int total_quantums=0;
int check_termination = -1;
bool check_block = false;

//---------------------User thread Functions-----------------------------

void scheduler_rr();

/*timer handler*/
void timer_handler(int sig){
  if(sig == SIGVTALRM){
    scheduler_rr();
  }
}

void set_virtual_time(){
  timer.it_value.tv_sec = library_quantum / CONVERT_SECOND_TO_MICRO; //seconds
  timer.it_value.tv_usec = (library_quantum % CONVERT_SECOND_TO_MICRO); //micro
  timer.it_interval.tv_sec = library_quantum / CONVERT_SECOND_TO_MICRO;
  timer.it_interval.tv_usec = (library_quantum % CONVERT_SECOND_TO_MICRO);
}

void acquire_mutex(){
  user_threads[current_running_thread]->wants_mutex=false;
  user_threads[current_running_thread]->blocked_by_mutex=false;
  mutex_lock.taken_by_id= current_running_thread;
  mutex_lock.is_locked = true;
}

/* scheduling threads with RR algorithm*/
void scheduler_rr(){
  int value=0;
  if(!check_block && check_termination ==-1){
    user_threads[current_running_thread]->thread_state=STATE_READY;
    ready_queue.push_back(current_running_thread);
  }
  if(check_termination < 0 ){
    check_block=false;
    value = sigsetjmp(user_threads[current_running_thread]->env, 1);
  }
  if(check_termination!=-1 && value !=0){
    delete user_threads[check_termination];
    user_threads[check_termination] = nullptr;
    user_threads.erase(check_termination);
    check_termination = -1;
  }
  if(value!=0){ //error caught
    return;
  }
  if(!ready_queue.empty()){
    //get new ready thread
    current_running_thread=ready_queue.front();
    ready_queue.pop_front();
    user_threads[current_running_thread]->thread_state=STATE_RUNNING;

  }
  total_quantums++;
  user_threads[current_running_thread]->running_quantom_times++;
  set_virtual_time();
  if (setitimer(ITIMER_VIRTUAL, &timer, nullptr)) {
    cerr << "error" << endl;
    exit(1);
  }
  //perform switch
  siglongjmp(user_threads[current_running_thread]->env,
      current_running_thread);
}

void stop_timer(){
  timer.it_value.tv_sec = 0;
  timer.it_value.tv_usec =0;
  timer.it_interval.tv_sec = 0;
  timer.it_interval.tv_usec = 0;
}

int main_thread_init(int &new_id){
  try {
    Uthread *uthread = new Uthread(new_id, nullptr);
    user_threads[new_id]=uthread;
    current_running_thread = 0;
  }
  catch(bad_alloc &ba){
    throw(ba);
  }
}

/* find id for new thread*/
int get_id_for_new_thread(){
  int find_id = FAIL;
  for(int i=0; i<MAX_THREAD_NUM;i++){
    if(user_threads[i] == nullptr){
      //smallest id is available
      find_id = i;
      break;
    }
  }
  return find_id;
}

/*
 * Description: This function initializes the thread library.
 * You may assume that this function is called before any other thread library
 * function, and that it is called exactly once. The input to the function is
 * the length of a quantum in micro-seconds. It is an error to call this
 * function with non-positive quantum_usecs.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_init(int quantum_usecs) {
  if(sigemptyset(&signals_set)){
    cerr << SYSTEM_CALL_FAIL<< SIGEMPTY_ERROR << endl;
    exit(FAILED_EXIT);
  }
  if(sigaddset(&signals_set,SIGVTALRM)){
    cerr << SYSTEM_CALL_FAIL<< SIGADD_ERROR << endl;
    exit(FAILED_EXIT);
  }
  if(quantum_usecs <= NON_POSITIVE_QUANT ){
    cerr << LIBRARY_FAIL << BAD_INPUT_NUM << endl;
    return FAIL;
  }
  int new_id = get_id_for_new_thread();
  library_quantum = quantum_usecs;
  try {
    total_quantums++;
    main_thread_init(new_id);
  }
  catch(bad_alloc &ba){
    cerr <<SYSTEM_CALL_FAIL<<ba.what()<<endl;
    exit(FAILED_EXIT);
  }
  sa.sa_handler = &timer_handler;
  if (sigaction(SIGVTALRM, &sa, nullptr) < 0) {
    cerr << SYSTEM_CALL_FAIL<< SIGACT_ERROR << endl;
    exit(FAILED_EXIT);
  }
  set_virtual_time();
  if (setitimer(ITIMER_VIRTUAL, &timer, nullptr)) {
    cerr << SYSTEM_CALL_FAIL<<  SETIME_ERROR << endl;
    exit(FAILED_EXIT);
  }
  return SUCCESS_INIT;
}
/*
 * Description: This function creates a new thread, whose entry point is the
 * function f with the signature void f(void). The thread is added to the end
 * of the READY threads list. The uthread_spawn function should fail if it
 * would cause the number of concurrent threads to exceed the limit
 * (MAX_THREAD_NUM). Each thread should be allocated with a stack of size
 * STACK_SIZE bytes.
 * Return value: On success, return the ID of the created thread.
 * On failure, return -1.
*/
int uthread_spawn(void (*f)(void)){
  if(sigprocmask(SIG_SETMASK,&signals_set, nullptr)){
    cerr<< SYSTEM_CALL_FAIL <<SIGPRO_ERROR<< endl;
    exit(FAILED_EXIT);
  }
  int new_id= get_id_for_new_thread();
  if(!f){
    cerr<<LIBRARY_FAIL <<BAD_POINTER<< endl;
    sigprocmask(SIG_UNBLOCK,&signals_set, nullptr);
    return FAIL;
  }
  if(new_id<0){
    cerr<<LIBRARY_FAIL << MAX_THREADS_ERROR<< endl;
    sigprocmask(SIG_UNBLOCK,&signals_set, nullptr);
    return FAIL;
  }
  try {
    Uthread *thread = new Uthread(new_id, f);
    ready_queue.push_back(new_id);
    user_threads[new_id]=thread;
  }
  catch(bad_alloc &ba){
    cerr<< SYSTEM_CALL_FAIL<<ba.what()<< endl;
    sigprocmask(SIG_UNBLOCK,&signals_set, nullptr);
    exit(FAILED_EXIT);
  }
  if(sigprocmask(SIG_UNBLOCK,&signals_set, nullptr)){
    cerr<< SYSTEM_CALL_FAIL<< SIGPRO_ERROR<< endl;
    exit(FAILED_EXIT);
  }
  return new_id;
}

int terminate_whole_process(){
  stop_timer();
  if(sigprocmask(SIG_UNBLOCK,&signals_set, nullptr)){
    cerr<< SYSTEM_CALL_FAIL<< SIGPRO_ERROR<< endl;
    exit(FAILED_EXIT);
  }
  for(int i=0; i<user_threads.size();i++){
    if(current_running_thread != i ){
      delete user_threads[i];
      user_threads[i]=nullptr;
    }
  }
  return EXIT_SUCCESS;
}
/*
 * Description: This function terminates the thread with ID tid and deletes
 * it from all relevant control structures. All the resources allocated by
 * the library for this thread should be released. If no thread with ID tid
 * exists it is considered an error. Terminating the main thread
 * (tid == 0) will result in the termination of the entire process using
 * exit(0) [after releasing the assigned library memory].
 * Return value: The function returns 0 if the thread was successfully
 * terminated and -1 otherwise. If a thread terminates itself or the main
 * thread is terminated, the function does not return.
*/
int uthread_terminate(int tid){
  if(sigprocmask(SIG_BLOCK,&signals_set, nullptr)){
    cerr<< SYSTEM_CALL_FAIL<< SIGPRO_ERROR<< endl;
    exit(FAILED_EXIT);
  }
  if(!user_threads[tid] || user_threads.find(tid)==user_threads.end()){
    cerr<<LIBRARY_FAIL <<NO_THREAD<< endl;
    sigprocmask(SIG_UNBLOCK,&signals_set, nullptr);
    return FAIL;
  }
  if(tid == MAIN_THREAD){
    terminate_whole_process();
    mutex_lock.taken_by_id= DEFAULT_MUTEX;
    mutex_lock.is_locked=false;
    delete user_threads[current_running_thread];
    user_threads[current_running_thread]= nullptr;
    exit(SUCCESS);
  }
  else if(user_threads[tid]->thread_state==STATE_READY){
    for( auto i=ready_queue.begin(); i!=ready_queue.end();){
      if(*i == tid) {
        i = ready_queue.erase(i);
        break;
      }
      else
        ++i;
    }
  }
  else if(user_threads[tid]->thread_state==STATE_BLOCKED){
    blocked_threads.erase(user_threads[tid]);
  }
  else if(tid == current_running_thread){
    check_termination = tid;
    stop_timer();
    if(sigprocmask(SIG_UNBLOCK,&signals_set, nullptr)){
      cerr<< SYSTEM_CALL_FAIL<< SIGPRO_ERROR<< endl;
      exit(1);
    }
    scheduler_rr();
  }
  delete user_threads[tid];
  user_threads[tid]= nullptr;
  user_threads.erase(tid);
  if(sigprocmask(SIG_UNBLOCK,&signals_set, nullptr)){
    cerr<< SYSTEM_CALL_FAIL<< SIGPRO_ERROR<< endl;
    exit(FAILED_EXIT);
  }
  return EXIT_SUCCESS;
}
/*
 * Description: This function blocks the thread with ID tid. The thread may
 * be resumed later using uthread_resume. If no thread with ID tid exists it
 * is considered as an error. In addition, it is an error to try blocking the
 * main thread (tid == 0). If a thread blocks itself, a scheduling decision
 * should be made. Blocking a thread in BLOCKED state has no
 * effect and is not considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_block(int tid){
  if(sigprocmask(SIG_BLOCK,&signals_set, nullptr)){
    cerr<< SYSTEM_CALL_FAIL<< SIGPRO_ERROR<< endl;
    exit(FAILED_EXIT);
  }
  if(user_threads.find(tid)==user_threads.end() || !user_threads[tid]){
    cerr<<LIBRARY_FAIL <<NO_THREAD<< endl;
    sigprocmask(SIG_UNBLOCK,&signals_set, nullptr);
    return FAIL;
  }
  if(tid == MAIN_THREAD){
    cerr<<LIBRARY_FAIL <<BLOCK_MAIN<< endl;
    sigprocmask(SIG_UNBLOCK,&signals_set, nullptr);
    return FAIL;
  }
  if(user_threads[tid]->thread_state==STATE_READY){
    for( auto i=ready_queue.begin(); i!=ready_queue.end();){
      if(*i == tid) {
        i = ready_queue.erase(i);
        break;
      }
      else
        ++i;
    }
  }
  if(user_threads[tid]->thread_state==STATE_BLOCKED){
    if(sigprocmask(SIG_UNBLOCK,&signals_set, nullptr)){
      cerr<< SYSTEM_CALL_FAIL<< SIGPRO_ERROR<< endl;
      exit(1);
    }
    return EXIT_SUCCESS;
  }
  else{
    if(tid == current_running_thread){
      user_threads[tid]->thread_state = STATE_BLOCKED;
      user_threads[tid]->blocked_by_func=true;
      check_block=true;
      blocked_threads.insert(user_threads[current_running_thread]);
      stop_timer();
      if(sigprocmask(SIG_UNBLOCK,&signals_set, nullptr)){
        cerr<< SYSTEM_CALL_FAIL<< SIGPRO_ERROR<< endl;
        exit(FAILED_EXIT);
      }
      scheduler_rr();
      return EXIT_SUCCESS;
    }
  }
  user_threads[tid]->thread_state=STATE_BLOCKED;
  user_threads[tid]->blocked_by_func=true;
  blocked_threads.insert(user_threads[tid]);
  if(sigprocmask(SIG_UNBLOCK,&signals_set, nullptr)){
    cerr<< SYSTEM_CALL_FAIL<< SIGPRO_ERROR<< endl;
    exit(FAILED_EXIT);
  }
  return EXIT_SUCCESS;
}
/*
 * Description: This function resumes a blocked thread with ID tid and moves
 * it to the READY state if it's not synced. Resuming a thread in a RUNNING or READY state
 * has no effect and is not considered as an error. If no thread with
 * ID tid exists it is considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_resume(int tid){
  if(user_threads.find(tid)==user_threads.end() || !user_threads[tid]){
    cerr<<LIBRARY_FAIL <<NO_THREAD<< endl;
    return FAIL;
  }
  if(user_threads[tid]->thread_state==STATE_RUNNING ||
  user_threads[tid]->thread_state==STATE_READY){
    return EXIT_SUCCESS;
  }
  user_threads[tid]->blocked_by_func=false;
  if(user_threads[tid]->blocked_by_mutex){
    EXIT_SUCCESS; //no effect
  }
  else {
    user_threads[tid]->thread_state = STATE_READY;
    ready_queue.push_back(tid);
    user_threads[tid]->blocked_by_func=false;
    blocked_threads.erase(user_threads[tid]);
  }
  return EXIT_SUCCESS;
}
/*
 * Description: This function tries to acquire a mutex.
 * If the mutex is unlocked, it locks it and returns.
 * If the mutex is already locked by different thread, the thread moves to BLOCK state.
 * In the future when this thread will be back to RUNNING state,
 * it will try again to acquire the mutex.
 * If the mutex is already locked by this thread, it is considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_mutex_lock(){
  sigprocmask(SIG_BLOCK,&signals_set, nullptr);
  if(!mutex_lock.is_locked){
    mutex_lock.is_locked=true;
    mutex_lock.taken_by_id=current_running_thread;
  }
  else {
    if (current_running_thread != mutex_lock.taken_by_id) {
//      user_threads[current_running_thread]->blocked_by_mutex = true;
//      uthread_block(current_running_thread); //??
//      user_threads[current_running_thread]->blocked_by_func = false;
//      user_threads[current_running_thread]->wants_mutex = true;
      user_threads[current_running_thread]->thread_state=STATE_BLOCKED;
      user_threads[current_running_thread]->blocked_by_mutex = true;
      scheduler_rr();
      //uthread_resume(current_running_thread);
      if(!mutex_lock.is_locked) {
        acquire_mutex();
      }

    }
    else{
      //error locked by this thread
      cerr<<LIBRARY_FAIL <<MUTEX_LOCKED_ERROR<< endl;
      sigprocmask(SIG_UNBLOCK,&signals_set, nullptr);
      return FAIL;
    }
  }
  sigprocmask(SIG_UNBLOCK,&signals_set, nullptr);
  return EXIT_SUCCESS;
}
/*
 * Description: This function releases a mutex.
 * If there are blocked threads waiting for this mutex,
 * one of them (no matter which one) moves to READY state.
 * If the mutex is already unlocked, it is considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_mutex_unlock(){
  sigprocmask(SIG_BLOCK,&signals_set, nullptr);
  bool found_blocked= false;
  if(!mutex_lock.is_locked){
    cerr<<LIBRARY_FAIL <<MUTEX_NOTLOCKED_ERROR<< endl;
    sigprocmask(SIG_UNBLOCK,&signals_set, nullptr);
    return FAIL;
  }
  if(current_running_thread==mutex_lock.taken_by_id) {
    for (const auto &blocked: blocked_threads) {
      if (blocked->blocked_by_mutex) {
        if (blocked->blocked_by_func) {
          //dont put to ready
          blocked->blocked_by_mutex = false;
          blocked->wants_mutex = true;
          found_blocked=true;
          break;
        } else {
          //READY STATE
          uthread_resume(current_running_thread);
          blocked->blocked_by_mutex = false;
          blocked->wants_mutex = true;
          found_blocked=true;
          break;
        }
      }
    }
    if(!found_blocked){
      mutex_lock.is_locked= false;
      mutex_lock.taken_by_id=DEFAULT_MUTEX;
    }
    user_threads[current_running_thread]->wants_mutex = false;
    user_threads[current_running_thread]->blocked_by_mutex=false;
  }
  else{
    sigprocmask(SIG_UNBLOCK,&signals_set, nullptr);
    cerr<<LIBRARY_FAIL <<MUTEX_NOTLOCKED_ERROR<< endl;
    return FAIL;
  }
  sigprocmask(SIG_UNBLOCK,&signals_set, nullptr);
  return EXIT_SUCCESS;
}
/*
 * Description: This function returns the thread ID of the calling thread.
 * Return value: The ID of the calling thread.
*/
int uthread_get_tid(){
  return current_running_thread;
}
/*
 * Description: This function returns the total number of quantums since
 * the library was initialized, including the current quantum.
 * Right after the call to uthread_init, the value should be 1.
 * Each time a new quantum starts, regardless of the reason, this number
 * should be increased by 1.
 * Return value: The total number of quantums.
*/
int uthread_get_total_quantums(){
  return total_quantums;
}
/*
 * Description: This function returns the number of quantums the thread with
 * ID tid was in RUNNING state. On the first time a thread runs, the function
 * should return 1. Every additional quantum that the thread starts should
 * increase this value by 1 (so if the thread with ID tid is in RUNNING state
 * when this function is called, include also the current quantum). If no
 * thread with ID tid exists it is considered an error.
 * Return value: On success, return the number of quantums of the thread with ID tid.
 * 			     On failure, return -1.
*/
int uthread_get_quantums(int tid){
  if(!user_threads[tid] || user_threads.find(tid)==user_threads.end()){
    cerr<<LIBRARY_FAIL <<NO_THREAD<< endl;
    return FAIL;
  }
  return (int)user_threads[tid]->running_quantom_times;
}
