/******************************************************************************
 * Based on the CRTurnQueue code from ConcurrencyFreaks
 * Modified and integrated to the benchmark by Ruslan Nikolaev
 * Copyright (c) 2019, Ruslan Nikolaev
 * All rights reserved.
 *
 * Copyright (c) 2014-2016, Pedro Ramalhete, Andreia Correia
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Concurrency Freaks nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************
 */

#ifndef _CRTURNQUEUE_H_
#define _CRTURNQUEUE_H_

#include <atomic>
#include "Harness.hpp"
#include "ConcurrentPrimitives.hpp"
#include "RUnorderedMap.hpp"
#include "HazardTracker.hpp"
#include "MemoryTracker.hpp"
#include "RetiredMonitorable.hpp"
#include <stdexcept>

#ifdef NGC
#define COLLECT false
#else
#define COLLECT true
#endif

template <class K, class V>
class CRTurnQueue : public RUnorderedMap<K, V>, public RetiredMonitorable {

private:
    struct Node {
        V item;
        const int enqTid;
        std::atomic<int> deqTid;
        std::atomic<Node*> next;

        Node(int tid) : enqTid{tid}, deqTid{IDX_NONE}, next{nullptr} { }

        Node(V _item, int tid) : item{_item}, enqTid{tid}, deqTid{IDX_NONE}, next{nullptr} { }

        inline bool deletable() {return true;}

        bool casDeqTid(int cmp, int val) {
     	    return deqTid.compare_exchange_strong(cmp, val);
        }
    };

    static const int IDX_NONE = -1;
    static const int MAX_THREADS = 128;
    const int maxThreads;

    MemoryTracker<Node>* memory_tracker;

    // To make sure we are not affected by the misaligned object
    alignas(128) int __pad1;

    // Pointers to head and tail of the list
    alignas(128) std::atomic<Node*> head;
    alignas(128) std::atomic<Node*> tail;
    // Enqueue requests
    alignas(128) std::atomic<Node*> enqueuers[MAX_THREADS];
    // Dequeue requests
    alignas(128) std::atomic<Node*> deqself[MAX_THREADS];
    alignas(128) std::atomic<Node*> deqhelp[MAX_THREADS];

    // To make sure we are not affected by the misaligned object
    alignas(128) int __pad2;

    const int kHpTail = 0;
    const int kHpHead = 0;
    const int kHpNext = 1;
    const int kHpDeq = 2;

    Node* sentinelNode;


    inline Node* reserveNode(Node* obj, int index, int tid, Node* node){
        memory_tracker->reserve_slot(obj, index, tid, node);
        return obj;
    }

    /**
     * Called only from dequeue()
     *
     * Search for the next request to dequeue and assign it to lnext.deqTid
     * It is only a request to dequeue if deqself[i] equals deqhelp[i].
     */
    int searchNext(Node* lhead, Node* lnext) {
        const int turn = lhead->deqTid.load();
        for (int idx=turn+1; idx < turn+maxThreads+1; idx++) {
            const int idDeq = idx%maxThreads;
            if (deqself[idDeq].load() != deqhelp[idDeq].load()) continue;
            if (lnext->deqTid.load() == IDX_NONE) lnext->casDeqTid(IDX_NONE, idDeq);
            break;
        }
        return lnext->deqTid.load();
    }


    /**
     * Called only from dequeue()
     *
     * If the ldeqTid is not our own, we must use an HP to protect against
     * deqhelp[ldeqTid] being retired-deleted-newed-reenqueued.
     */
    void casDeqAndHead(Node* lhead, Node* lnext, const int tid) {
        const int ldeqTid = lnext->deqTid.load();
        if (ldeqTid == tid) {
            deqhelp[ldeqTid].store(lnext, std::memory_order_release);
        } else {
            Node* ldeqhelp = reserveNode(deqhelp[ldeqTid].load(), kHpDeq, tid, nullptr);
            if (ldeqhelp != lnext && lhead == head.load()) {
                deqhelp[ldeqTid].compare_exchange_strong(ldeqhelp, lnext); // Assign next to request
            }
        }
        head.compare_exchange_strong(lhead, lnext);
    }


    /**
     * Called only from dequeue()
     *
     * Giveup procedure, for when there are no nodes left to dequeue
     */
    void giveUp(Node* myReq, const int tid) {
        Node* lhead = head.load();
        if (deqhelp[tid].load() != myReq || lhead == tail.load()) return;
        reserveNode(lhead, kHpHead, tid, nullptr);
	if (lhead != head.load()) return;
        Node* lnext = reserveNode(lhead->next.load(), kHpNext, tid, lhead);
        if (lhead != head.load()) return;
        if (searchNext(lhead, lnext) == IDX_NONE) lnext->casDeqTid(IDX_NONE, tid);
        casDeqAndHead(lhead, lnext, tid);
    }

public:
   CRTurnQueue(GlobalTestConfig* gtc):
           RetiredMonitorable(gtc),maxThreads(gtc->task_num) {
        int epochf = gtc->getEnv("epochf").empty()? 150:stoi(gtc->getEnv("epochf"));
        int emptyf = gtc->getEnv("emptyf").empty()? 30:stoi(gtc->getEnv("emptyf"));
        std::cout<<"emptyf:"<<emptyf<<std::endl;
        memory_tracker = new MemoryTracker<Node>(gtc, epochf, emptyf, 3, COLLECT);
        memory_tracker->start_op(0);
        sentinelNode = mkNode(0);
        head.store(sentinelNode, std::memory_order_relaxed);
        tail.store(sentinelNode, std::memory_order_relaxed);
        for (int i = 0; i < maxThreads; i++) {
            enqueuers[i].store(nullptr, std::memory_order_relaxed);
            // deqself[i] != deqhelp[i] means that isRequest=false
            deqself[i].store(mkNode(0), std::memory_order_relaxed);
            deqhelp[i].store(mkNode(0), std::memory_order_relaxed);
        }
        memory_tracker->end_op(0);
        memory_tracker->clear_all(0);
    }


    ~CRTurnQueue() {
        optional<V> nullres={};
        memory_tracker->start_op(0);
        memory_tracker->reclaim(sentinelNode, 0);
        while (remove(0, 0) != nullres); // Drain the queue
        for (int i=0; i < maxThreads; i++) memory_tracker->reclaim(deqself[i].load(), 0);
        for (int i=0; i < maxThreads; i++) memory_tracker->reclaim(deqhelp[i].load(), 0);
	memory_tracker->end_op(0);
        memory_tracker->clear_all(0);
    }

    Node* mkNode(int tid){
        void* ptr = memory_tracker->alloc(tid);
        return new (ptr) Node(tid);
    }

    Node* mkNode(V item, int tid){
        void* ptr = memory_tracker->alloc(tid);
        return new (ptr) Node(item, tid);
    }

    /**
     * Steps when uncontended:
     * 1. Add node to enqueuers[]
     * 2. Insert node in tail.next using a CAS
     * 3. Advance tail to tail.next
     * 4. Remove node from enqueuers[]
     *
     * @param tid The tid must be a UNIQUE index for each thread, in the range 0 to maxThreads-1
     */
    bool insert(K __key, V item, int tid) {
        Node* myNode = mkNode(item,tid);
	collect_retired_size(memory_tracker->get_retired_cnt(tid), tid);
        memory_tracker->start_op(tid);
        enqueuers[tid].store(myNode);
        for (int i = 0; i < maxThreads; i++) {
            if (enqueuers[tid].load() == nullptr) {
                memory_tracker->end_op(tid);
                memory_tracker->clear_all(tid);
                return true; // Some thread did all the steps
            }
            Node* ltail = reserveNode(tail.load(), kHpTail, tid, nullptr);
            if (ltail != tail.load()) continue; // If the tail advanced maxThreads times, then my node has been enqueued
            if (enqueuers[ltail->enqTid].load() == ltail) {  // Help a thread do step 4
                Node* tmp = ltail;
                enqueuers[ltail->enqTid].compare_exchange_strong(tmp, nullptr);
            }
            for (int j = 1; j < maxThreads+1; j++) {         // Help a thread do step 2
                Node* nodeToHelp = enqueuers[(j + ltail->enqTid) % maxThreads].load();
                if (nodeToHelp == nullptr) continue;
                Node* nodenull = nullptr;
                ltail->next.compare_exchange_strong(nodenull, nodeToHelp);
                break;
            }
            Node* lnext = ltail->next.load();
     	    if (lnext != nullptr) tail.compare_exchange_strong(ltail, lnext); // Help a thread do step 3
        }
        enqueuers[tid].store(nullptr, std::memory_order_release); // Do step 4, just in case it's not done
        memory_tracker->end_op(tid);
        memory_tracker->clear_all(tid);
        return true;
    }


    /**
     * Steps when uncontended:
     * 1. Publish request to dequeue in dequeuers[tid];
     * 2. CAS node->deqTid from IDX_START to tid;
     * 3. Set dequeuers[tid] to the newly owned node;
     * 4. Advance the head with casHead();
     *
     * We must protect either head or tail with HP before doing the check for
     * empty queue, otherwise we may get into retired-deleted-newed-reenqueued.
     *
     * @param tid: The tid must be a UNIQUE index for each thread, in the range 0 to maxThreads-1
     */
    optional<V> remove(K __key, int tid) {
        optional<V> res={};
	collect_retired_size(memory_tracker->get_retired_cnt(tid), tid);
	memory_tracker->start_op(tid);
        Node* prReq = deqself[tid].load();     // Previous request
        Node* myReq = deqhelp[tid].load();
        deqself[tid].store(myReq);             // Step 1
        for (int i=0; i < maxThreads; i++) {
            if (deqhelp[tid].load() != myReq) break; // No need for HP
            Node* lhead = reserveNode(head.load(), kHpHead, tid, nullptr);
            if (lhead != head.load()) continue;
            if (lhead == tail.load()) {        // Give up
                deqself[tid].store(prReq);     // Rollback request to dequeue
                giveUp(myReq, tid);
                if (deqhelp[tid].load() != myReq) {
                    deqself[tid].store(myReq, std::memory_order_relaxed);
                    break;
                }
                memory_tracker->end_op(tid);
                memory_tracker->clear_all(tid);
                return res;
            }
            Node* lnext = reserveNode(lhead->next.load(), kHpNext, tid, lhead);
            if (lhead != head.load()) continue;
 		    if (searchNext(lhead, lnext) != IDX_NONE) casDeqAndHead(lhead, lnext, tid);
        }
        Node* myNode = deqhelp[tid].load();
        Node* lhead = reserveNode(head.load(), kHpHead, tid, nullptr);     // Do step 4 if needed
        if (lhead == head.load() && myNode == lhead->next.load()) head.compare_exchange_strong(lhead, myNode);
	res = myNode->item;
        memory_tracker->retire(prReq, tid);
        memory_tracker->end_op(tid);
        memory_tracker->clear_all(tid);
        return res;
    }

public:
    optional<V> get(K key, int tid)
    {
        optional<V> res={};
        return res;
    }

    optional<V> put(K key, V val, int tid)
    {
        optional<V> res={};
        return res;
    }

    optional<V> replace(K key, V val, int tid)
    {
        optional<V> res={};
        return res;
    }
};

template <class K, class V> 
class CRTurnQueueFactory : public RideableFactory {
    CRTurnQueue<K,V>* build(GlobalTestConfig* gtc) {
        // FIXME: better be aligned
        // it should still be OK as is;
        // extra __pad1, __pad2 fields take care of misalignments
        return new CRTurnQueue<K,V>(gtc);
    }
};

#endif /* _CRTURNQUEUE_H_ */
