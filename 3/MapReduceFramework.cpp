
/*******************************includes****************************/

#include <atomic>
#include <pthread.h>
#include <utility>
#include <iostream>
#include <unistd.h>
//#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include "Barrier.h"
#include <algorithm>
#include <semaphore.h>

/*****************************classes*******************************/
using namespace std;
#define EXIT_FAIL 1

class ThreadContext{
 public:
  ThreadContext(){}
  ThreadContext(const MapReduceClient& client,
                const InputVec& inputVec, OutputVec& outputVec,
                int multiThreadLevel, int thread_id) try:
      input_vec_(&inputVec),
      client_(&client),
      output_vec_(&outputVec),
      thread_id_(thread_id),
      threads_size_(multiThreadLevel),
      shuffle_barrier_(new Barrier(multiThreadLevel)),
      barrier_(new Barrier(multiThreadLevel)),
      shuffle_sem_(new sem_t),
      reduce_sem(new sem_t),
      state_mutex(new pthread_mutex_t()),
      reduce_mutex(new pthread_mutex_t()),
      output_mutex(new pthread_mutex_t()),
      p_intermediate_vec_(new IntermediateVec* [multiThreadLevel]),
      shuffle_process(new vector<IntermediateVec*>),
      atomic_map_count(new atomic<unsigned long>(0)),
      atomic_shuffle_count(new atomic<unsigned long>(0)),
      count_atomic(new atomic<unsigned long>(0)),
      atomic_reduce_count(new atomic<unsigned long>(0)),
      atomic_total_shuffle(new atomic<unsigned long>(0)){
    if(sem_init(shuffle_sem_,0,0) != 0 || sem_init(reduce_sem,0,0)!= 0){
      cerr <<"system error: sem_init failed operation" << endl;
      exit(EXIT_FAIL);
    }
    if(pthread_mutex_init(state_mutex, nullptr)!=0 ||
        pthread_mutex_init(reduce_mutex, nullptr)!= 0 ||
        pthread_mutex_init(output_mutex, nullptr)!=0){
      cerr <<"system error: pthread_mutex_init failed operation" << endl;
      exit(EXIT_FAIL);
    }

  }
  catch (bad_alloc &ba){
    cerr <<"system error: "<< ba.what() << endl;
    exit(EXIT_FAIL);
  }
  const InputVec *input_vec_;
  const MapReduceClient *client_;
  OutputVec * output_vec_;
  int thread_id_;
  int *intermediate_pairs_sum_{};
  int stage_ = UNDEFINED_STAGE;
  int threads_size_;
  Barrier * shuffle_barrier_;
  Barrier *barrier_;
  sem_t * shuffle_sem_;
  sem_t * reduce_sem;
  pthread_mutex_t * state_mutex;
  pthread_mutex_t * reduce_mutex;
  pthread_mutex_t * output_mutex;
  IntermediateVec  ** p_intermediate_vec_;
  vector<IntermediateVec *> *shuffle_process;
  atomic<unsigned long> *atomic_map_count;
  atomic<unsigned long> *atomic_shuffle_count;
  atomic<unsigned long> *count_atomic;
  atomic<unsigned long> *atomic_reduce_count;
  atomic<unsigned long> *atomic_total_shuffle;
  //bool is_done = false;
};

typedef struct jobInfo{
  pthread_t * threads;
  int threads_size;
  bool ji_wait;
  bool is_done;
  pthread_mutex_t *mutex_wait;
  ThreadContext * tc;
}jobInfo;

//for sort func
bool K2Compare(IntermediatePair lhs, IntermediatePair rhs){
  return *lhs.first < *rhs.first;
}

void * sem_update_reduce(ThreadContext * thread_context){
  for(int i=0; i<thread_context->threads_size_;i++){
    if(sem_post(thread_context->reduce_sem)!=0){
      cerr <<"system error: sem_post failed operation" << endl;
      exit(EXIT_FAIL);
    }
  }
}

void * sem_update_shuffle(ThreadContext * thread_context){
  for(int i=0; i<thread_context->threads_size_;i++){
    if(sem_post(thread_context->shuffle_sem_)!=0){
      cerr <<"system error: sem_post failed operation" << endl;
      exit(EXIT_FAIL);
    }
  }
}

void map_phase(void* arg){
  auto * thread_c =(ThreadContext*)arg;
  if(thread_c->thread_id_==0){
    if(pthread_mutex_lock(thread_c->state_mutex)!=0){
      cerr <<"system error: pthread_mutex_lock failed operation" << endl;
      exit(EXIT_FAIL);
    }
    thread_c->stage_=MAP_STAGE;
    if(pthread_mutex_unlock(thread_c->state_mutex)!=0){
      cerr <<"system error: pthread_mutex_unlock failed operation" << endl;
      exit(EXIT_FAIL);
    }
  }
  unsigned long old_val= (*(thread_c->count_atomic))++;
  while( old_val < thread_c->input_vec_->size()){
    thread_c->client_->map(thread_c->input_vec_->at(old_val).first,
                           thread_c->input_vec_->at(old_val).second, thread_c);
    //count mapped pairs
    (*(thread_c->atomic_map_count))++;
    old_val=(*(thread_c->count_atomic))++;
  }
}

void reduce_phase(void *arg){
  auto * thread_c =(ThreadContext*)arg;
  if(pthread_mutex_lock(thread_c->state_mutex)!=0){
    cerr <<"system error: pthread_mutex_lock failed operation" << endl;
    exit(EXIT_FAIL);
  }
  thread_c->stage_=REDUCE_STAGE;
  if(pthread_mutex_unlock(thread_c->state_mutex)!=0){
    cerr <<"system error: pthread_mutex_unlock failed operation" << endl;
    exit(EXIT_FAIL);
  }
  while(true){
    if(sem_wait(thread_c->reduce_sem)!=0){
      cerr <<"system error: pthread_mutex_unlock failed operation" << endl;
      exit(EXIT_FAIL);
    }
    if(pthread_mutex_lock(thread_c->reduce_mutex)!=0){
      cerr <<"system error: pthread_mutex_lock failed operation" << endl;
      exit(EXIT_FAIL);
    }
    if(thread_c->shuffle_process->empty()){
      if(pthread_mutex_unlock(thread_c->reduce_mutex)!=0){
        cerr <<"system error: pthread_mutex_unlock failed operation" << endl;
        exit(EXIT_FAIL);
      } return;
    }
    IntermediateVec *red_vec = thread_c->shuffle_process->back();
    thread_c->shuffle_process->pop_back(); //remove
    if(pthread_mutex_unlock(thread_c->reduce_mutex)!=0){
      cerr <<"system error: pthread_mutex_unlock failed operation" << endl;
      exit(EXIT_FAIL);
    }
    //perform reduce on the pooped vec
    thread_c->client_->reduce(red_vec, thread_c);
    //update atomic count
    (*(thread_c->count_atomic)) +=red_vec->size();
    delete red_vec;
  }
}

void * map_reduce(void *arg){
  auto * thread_context = (ThreadContext*)arg;
  //perform map stage
  map_phase(thread_context);
  bool is_shuffle_done = false;
  int threadId = thread_context->thread_id_;

  //perform sort stage after all mapped
  sort(thread_context->p_intermediate_vec_[threadId]->begin(),
       thread_context->p_intermediate_vec_[threadId]->end(), K2Compare);
  thread_context->barrier_->barrier();
  if(thread_context->thread_id_ == 0){
    if(pthread_mutex_lock(thread_context->output_mutex)!=0){
      cerr <<"system error: pthread_mutex_lock failed operation" << endl;
      exit(EXIT_FAIL);
    }
    *(thread_context->count_atomic)=0;
    thread_context->intermediate_pairs_sum_= new int(0);
    for(int i =0; i<thread_context->threads_size_; i++){
      *thread_context->intermediate_pairs_sum_ +=
          thread_context->p_intermediate_vec_[i]->size();
    }
    if(pthread_mutex_unlock(thread_context->output_mutex)!=0){
      cerr <<"system error: pthread_mutex_unlock failed operation" << endl;
      exit(EXIT_FAIL);
    }
    //now lets shuffle
    if(pthread_mutex_lock(thread_context->state_mutex)!=0){
      cerr <<"system error: pthread_mutex_lock failed operation" << endl;
      exit(EXIT_FAIL);
    }
    thread_context->stage_=SHUFFLE_STAGE;
    if(pthread_mutex_unlock(thread_context->state_mutex)!=0){
      cerr <<"system error: pthread_mutex_unlock failed operation" << endl;
      exit(EXIT_FAIL);
    }
    K2 * greatest = nullptr;
    for(int k=0; k<thread_context->threads_size_; k++){
      if(thread_context->p_intermediate_vec_[k]->empty()==true){
        continue;
      }
      if(greatest == nullptr||*greatest<*thread_context->p_intermediate_vec_[k
      ]->back().first){
        greatest= thread_context->p_intermediate_vec_[k]->back().first;
      }
    }
    while(greatest != nullptr){
      auto * red_vec = new IntermediateVec;
      for(int j=0; j<thread_context->threads_size_;j++){
        while(!thread_context->p_intermediate_vec_[j]->empty()
            && !(*greatest<*thread_context->p_intermediate_vec_[j]->back().first)
            && !(*thread_context->p_intermediate_vec_[j]->back().first<*greatest)){
          red_vec->push_back(thread_context->p_intermediate_vec_[j]->back());
          thread_context->p_intermediate_vec_[j]->pop_back();
          (*thread_context->atomic_shuffle_count)++;
        }
      }
      if(pthread_mutex_lock(thread_context->reduce_mutex)!=0){
        cerr <<"system error: pthread_mutex_lock failed operation" << endl;
        exit(EXIT_FAIL);
      }
      thread_context->shuffle_process->push_back(red_vec);
      if(pthread_mutex_unlock(thread_context->reduce_mutex)!=0){
        cerr <<"system error: pthread_mutex_unlock failed operation" << endl;
        exit(EXIT_FAIL);
      }
      if(sem_post(thread_context->reduce_sem)!=0){
        cerr <<"system error: sem_post failed operation" << endl;
        exit(EXIT_FAIL);
      }
      greatest = nullptr;
      for(int k=0; k<thread_context->threads_size_; k++){
        if(thread_context->p_intermediate_vec_[k]->empty()==true){
          continue;
        }
        if(greatest == nullptr ||
        *greatest<*thread_context->p_intermediate_vec_[k]->back().first){
          greatest= thread_context->p_intermediate_vec_[k]->back().first;
        }
      }
    }
    sem_update_reduce(thread_context);
    is_shuffle_done = true; //update so we know we finished here
    sem_update_shuffle(thread_context);
  }
  if(thread_context->thread_id_!=0 && !is_shuffle_done){
    sem_wait(thread_context->shuffle_sem_);
  }
  reduce_phase(thread_context);//now lets start reducing
  return nullptr;
}

void emit2 (K2* key, V2* value, void* context){
  auto * thread_context = (ThreadContext*)context;
  int id = thread_context->thread_id_;
  thread_context->p_intermediate_vec_[id]->push_back({key,value});
  (*thread_context->atomic_total_shuffle)++;
}

void emit3 (K3* key, V3* value, void* context){
  auto * thread_context = (ThreadContext*)context;
  if(pthread_mutex_lock(thread_context->output_mutex)!=0){
    cerr <<"system error: pthread_mutex_lock failed operation" << endl;
    exit(EXIT_FAIL);
  }
  thread_context->output_vec_->push_back({key,value});
  if(pthread_mutex_unlock(thread_context->output_mutex)!=0){
    cerr <<"system error: pthread_mutex_unlock failed operation" << endl;
    exit(EXIT_FAIL);
  }
}


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){

try {
//sem
  auto *reduce_sem = new sem_t;
  if (sem_init(reduce_sem, 0, 0) != 0) {
    std::cerr << "system error: sem_init failed operation" << std::endl;
    exit(EXIT_FAIL);
  }
  auto *shuffle_sem = new sem_t;
  if (sem_init(shuffle_sem, 0, 0) != 0) {
    std::cerr << "system error: sem_init failed operation" << std::endl;
    exit(EXIT_FAIL);
  }
//barrier
  auto *barrier = new Barrier(multiThreadLevel);
  auto *shuf_bar = new Barrier(multiThreadLevel);
//mutex
  auto *state_mutex = new pthread_mutex_t;
  if(pthread_mutex_init(state_mutex, nullptr)!=0){
    cerr <<"system error: pthread_mutex_lock failed operation" << endl;
    exit(EXIT_FAIL);
  }
  auto *reduce_mutex = new pthread_mutex_t;
  if(pthread_mutex_init(reduce_mutex, nullptr)!=0){
    cerr <<"system error: pthread_mutex_lock failed operation" << endl;
    exit(EXIT_FAIL);
  }
  auto *out_mutex = new pthread_mutex_t;
  if(pthread_mutex_init(out_mutex, nullptr)!=0){
    cerr <<"system error: pthread_mutex_init failed operation" << endl;
    exit(EXIT_FAIL);
  }
  auto *mutex_waiting = new pthread_mutex_t();
  if(pthread_mutex_init(mutex_waiting, nullptr)!=0){
    cerr <<"system error: pthread_mutex_init failed operation" << endl;
    exit(EXIT_FAIL);
  }

  auto *process_shuff = new vector<IntermediateVec *>;
  auto *map_counter = new atomic<unsigned long>(0);
  auto *count_atomic = new atomic<unsigned long>(0);
  auto *total_shuffle_work = new atomic<unsigned long>(0);
  auto *reduce_counter = new atomic<unsigned long>(0);
  auto *atomic_shuffle_counter = new atomic<unsigned long>(0);
  auto **intermediate_q = new IntermediateVec *[multiThreadLevel];
  auto *map_r_threads = new pthread_t[multiThreadLevel];
  auto *p_thread_context = new ThreadContext[multiThreadLevel];
  for (int i = 0; i < multiThreadLevel; ++i) {
    p_thread_context[i].input_vec_ = &inputVec;
    p_thread_context[0].stage_ = UNDEFINED_STAGE;
    p_thread_context[i].client_ = &client;
    p_thread_context[i].output_vec_ = &outputVec;
    p_thread_context[i].thread_id_ = i;
    p_thread_context[i].threads_size_ = multiThreadLevel;
    p_thread_context[i].shuffle_barrier_ = shuf_bar;
    p_thread_context[i].barrier_ = barrier;
    p_thread_context[i].shuffle_sem_ = shuffle_sem;
    p_thread_context[i].reduce_sem = reduce_sem;
    p_thread_context[i].state_mutex = state_mutex;
    p_thread_context[i].reduce_mutex = reduce_mutex;
    p_thread_context[i].output_mutex = out_mutex;
    p_thread_context[i].p_intermediate_vec_ = intermediate_q;
    p_thread_context[i].shuffle_process = process_shuff;
    p_thread_context[i].atomic_map_count = map_counter;
    p_thread_context[i].atomic_shuffle_count = atomic_shuffle_counter;
    p_thread_context[i].count_atomic = count_atomic;
    p_thread_context[i].atomic_reduce_count = reduce_counter;
    p_thread_context[i].atomic_total_shuffle = total_shuffle_work;
    intermediate_q[i] = new IntermediateVec();
  }
  for (int j = 0; j < multiThreadLevel; j++) {
    if (pthread_create(map_r_threads + j, nullptr, map_reduce, p_thread_context + j) != 0) {
      cerr << "system error: pthread_create failed operation" << endl;
      exit(EXIT_FAIL);
    }
  }
  auto *ji = new jobInfo{map_r_threads, multiThreadLevel, false, false,
                         mutex_waiting, p_thread_context};
  return (JobHandle)ji;
}
catch (bad_alloc &ba){
  cerr <<"system error: "<< ba.what() << endl;
  exit(EXIT_FAIL);
}
}

void waitForJob(JobHandle job){
  auto * job_context =(jobInfo*)job;
  if(pthread_mutex_lock(job_context->mutex_wait)!=0){
    cerr <<"system error: pthread_mutex_lock failed operation" << endl;
    exit(EXIT_FAIL);
  }
  if(job_context->ji_wait){
    cerr<< "system error: 2 threads called waitforjob at the same time"<<endl;
    if(pthread_mutex_unlock(job_context->mutex_wait)!=0){
      cerr <<"system error: pthread_mutex_unlock failed operation" << endl;
      exit(EXIT_FAIL);
    }
    return;
  }
  else{
    job_context->ji_wait=true;
  }

  for( int i=0; i<job_context->threads_size;i++){
    if(pthread_join(job_context->threads[i], nullptr)!=0){
      cerr <<"system error: pthread_join failed operation" << endl;
      exit(EXIT_FAIL);
    }
  }
  delete [] job_context->threads;
  job_context->threads=nullptr;
  if(pthread_mutex_unlock(job_context->mutex_wait)!=0){
        cerr <<"system error: pthread_mutex_unlock failed operation" << endl;
        exit(EXIT_FAIL);
  }
}

void getJobState(JobHandle job, JobState* state) {
  auto *jc = (jobInfo *) job;
  if(pthread_mutex_lock(jc->tc[0].state_mutex)!=0){
    cerr <<"system error: pthread_mutex_lock failed operation" << endl;
    exit(EXIT_FAIL);
  }
  if(pthread_mutex_lock(jc->tc[0].output_mutex)!=0){
    cerr <<"system error: pthread_mutex_lock failed operation" << endl;
    exit(EXIT_FAIL);
  }
  int s= jc->tc[0].stage_;
  if(jc->tc[0].input_vec_->empty()){state->percentage = 100; return;}
  switch(s){
    case UNDEFINED_STAGE:
      state->percentage=0;
      break;
    case MAP_STAGE:
      state->percentage= 100 * ((float)*jc->tc[0].atomic_map_count/
          jc->tc[0].input_vec_->size());
      break;
    case SHUFFLE_STAGE:
      state->percentage = 100 * (float)*(jc->tc[0].atomic_shuffle_count)/
          *(jc->tc[0].atomic_total_shuffle);;
      break;
    case REDUCE_STAGE:
      state->percentage = 100 *((float) *jc->tc[0].count_atomic /
          (float)*jc->tc[0].intermediate_pairs_sum_);
  }
  state->stage = (stage_t)jc->tc[0].stage_;
  if(pthread_mutex_unlock(jc->tc[0].output_mutex)!=0){
    cerr <<"system error: pthread_mutex_unlock failed operation" << endl;
    exit(EXIT_FAIL);
  }
  if(pthread_mutex_unlock(jc->tc[0].state_mutex)!=0){
    cerr <<"system error: pthread_mutex_unlock failed operation" << endl;
    exit(EXIT_FAIL);
  }
}

void closeJobHandle(JobHandle job){
  auto * job_context=(jobInfo*)job;
  job_context->is_done=true;
  //check threads
  if(job_context->threads != nullptr){
    waitForJob(job_context);
  }
  //barrier
  delete job_context->tc[0].barrier_;
  job_context->tc[0].barrier_ = nullptr;
  delete job_context->tc[0].shuffle_barrier_;
  job_context->tc[0].shuffle_barrier_ = nullptr;
//sem
  if (sem_destroy(job_context->tc[0].shuffle_sem_) != 0){
    std::cerr << "system error: sem_destroy failed operation" << std::endl;
    exit(EXIT_FAIL);
  }
  delete job_context->tc[0].shuffle_sem_;
  job_context->tc[0].shuffle_sem_= nullptr;
  if (sem_destroy(job_context->tc[0].reduce_sem) != 0){
    std::cerr << "system error: sem_destroy failed operation" << std::endl;
    exit(EXIT_FAIL);
  }
  delete job_context->tc[0].reduce_sem;
  job_context->tc[0].reduce_sem= nullptr;

  for(int i=0; i<job_context->threads_size; ++i){
    delete job_context->tc[i].p_intermediate_vec_[i];
    job_context->tc[i].p_intermediate_vec_[i]= nullptr;
  }
  delete[] job_context->tc[0].p_intermediate_vec_;
  job_context->tc[0].p_intermediate_vec_= nullptr;
  delete job_context->tc[0].shuffle_process;
  job_context->tc[0].shuffle_process= nullptr;
  delete job_context->tc[0].intermediate_pairs_sum_;
  job_context->tc[0].intermediate_pairs_sum_= nullptr;
  //atomic
  delete job_context->tc[0].atomic_map_count;
  job_context->tc[0].atomic_map_count= nullptr;
  delete job_context->tc[0].atomic_shuffle_count;
  job_context->tc[0].atomic_shuffle_count= nullptr;
  delete job_context->tc[0].count_atomic;
  job_context->tc[0].count_atomic= nullptr;
  delete job_context->tc[0].atomic_reduce_count;
  job_context->tc[0].atomic_reduce_count= nullptr;
  delete job_context->tc[0].atomic_total_shuffle;
  job_context->tc[0].atomic_total_shuffle= nullptr;
//mutex
  if(pthread_mutex_destroy(job_context->tc[0].state_mutex)!=0){
    std::cerr << "system error: pthread_mutex_destroy failed operation" <<
              std::endl;
    exit(EXIT_FAIL);
  }
  delete job_context->tc[0].state_mutex;
  job_context->tc[0].state_mutex= nullptr;
  if(pthread_mutex_destroy(job_context->tc[0].reduce_mutex)!=0){
    std::cerr << "system error: pthread_mutex_destroy failed operation" <<
              std::endl;
    exit(EXIT_FAIL);
  }
  delete job_context->tc[0].reduce_mutex;
  job_context->tc[0].reduce_mutex= nullptr;
  if(pthread_mutex_destroy(job_context->tc[0].output_mutex)!=0){
    std::cerr << "system error: pthread_mutex_destroy failed operation" <<
              std::endl;
    exit(EXIT_FAIL);
  }
  delete job_context->tc[0].output_mutex;
  job_context->tc[0].output_mutex= nullptr;
  delete[] job_context->tc;
  job_context->tc= nullptr;
  if(pthread_mutex_destroy(job_context->mutex_wait)!=0){
    std::cerr << "system error: pthread_mutex_destroy failed operation" <<
              std::endl;
    exit(EXIT_FAIL);
  }
  delete job_context->mutex_wait;
  job_context->mutex_wait= nullptr;
  delete job_context;
  job_context= nullptr;
}