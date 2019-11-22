/******************************************************************************
 * Based on the KoganPetrankQueueCHP code from ConcurrencyFreaks
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

#ifndef _WFQUEUE_H_
#define _WFQUEUE_H_

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
class WFQueue : public RUnorderedMap<K, V>, public RetiredMonitorable {

private:

    struct Node {
        V item; // Keep it as in OpDesc
        std::atomic<bool> itemDeletable; // Keep it as in OpDesc
        bool __pad0;
        bool  __pad1;
        const int enqTid;
        std::atomic<int> deqTid { IDX_NONE };
        std::atomic<Node*> next { nullptr };

        Node(V userItem, int enqTid) : item{userItem}, itemDeletable(false), enqTid{enqTid} { }
        Node(int enqTid) : itemDeletable(true), enqTid{enqTid} { }

        inline bool deletable() { return itemDeletable.load(); }

        bool casNext(Node* cmp, Node* val) {
            // Use a tmp variable because this CAS "replaces" the value of the first argument
            Node* tmp = cmp;
            return next.compare_exchange_strong(tmp, val);
        }
    };


    struct OpDesc { // Keep the size <= sizeof(Node)
        V __pad2; // Keep it as in Node
        std::atomic<bool> itemDeletable; // Keep it as in Node
        const bool pending;
        const bool enqueue;
        const long long phase;
        Node* node; // This is immutable once assigned
        OpDesc (long long ph, bool pend, bool enq, Node* n) : itemDeletable(true), pending{pend}, enqueue{enq}, phase{ph}, node{n} { }
    };


    bool casTail(Node *cmp, Node *val) {
        return tail.compare_exchange_strong(cmp, val);
    }

    bool casHead(Node *cmp, Node *val) {
        return head.compare_exchange_strong(cmp, val);
    }

    // Member variables
    static const int MAX_THREADS = 128;

    // To make sure we are not affected by the misaligned object
    alignas(128) int __pad1;

    // Pointers to head and tail of the list
    alignas(128) std::atomic<Node*> head;
    alignas(128) std::atomic<Node*> tail;
    // Array of enque and dequeue requests
    alignas(128) std::atomic<OpDesc*> state[MAX_THREADS];

    // To make sure we are not affected by the misaligned object
    alignas(128) int __pad2;

    const static int IDX_NONE = -1;
    OpDesc* OPDESC_END;
    const int maxThreads;

    const static int HP_CRT_REQ = 3;

    MemoryTracker<Node>* memory_tracker;

    const int kHpCurr = 0;
    const int kHpNext = 1;
    const int kHpPrev = 2;
    const int kHpODCurr = 3;
    const int kHpODNext = 4;

public:
   WFQueue(GlobalTestConfig* gtc):
           RetiredMonitorable(gtc),maxThreads(gtc->task_num) {
        int epochf = gtc->getEnv("epochf").empty()? 150:stoi(gtc->getEnv("epochf"));
        int emptyf = gtc->getEnv("emptyf").empty()? 30:stoi(gtc->getEnv("emptyf"));
        std::cout<<"emptyf:"<<emptyf<<std::endl;
        memory_tracker = new MemoryTracker<Node>(gtc, epochf, emptyf, 5, COLLECT);
        memory_tracker->start_op(0);
        OPDESC_END = mkOpDesc(IDX_NONE, false, true, nullptr, 0);
        Node* sentinelNode = mkNode(0);
        head.store(sentinelNode);
        tail.store(sentinelNode);
        for (int i = 0; i < maxThreads; i++) {
            state[i].store(OPDESC_END);
        }
        memory_tracker->end_op(0);
        memory_tracker->clear_all(0);
    }

    ~WFQueue() {
        optional<V> nullres={};
        while (remove(0, 0) != nullres); // Drain the queue
        memory_tracker->reclaim(head.load()); // Delete the last node
        memory_tracker->reclaim((Node *)OPDESC_END);
    }

    Node* mkNode(int TID){
        void* ptr = memory_tracker->alloc(TID);
        return new (ptr) Node(IDX_NONE);
    }

    Node* mkNode(V item, int TID){
        void* ptr = memory_tracker->alloc(TID);
        return new (ptr) Node(item, TID);
    }

    OpDesc* mkOpDesc(long long ph, bool pend, bool enq, Node* n, int TID){
        void* ptr = memory_tracker->alloc(TID);
        return new (ptr) OpDesc(ph, pend, enq, n);
    }

    inline OpDesc* readOpDesc(std::atomic<OpDesc*>& obj, int index, int tid, OpDesc* node){
        return (OpDesc*)memory_tracker->read(*((std::atomic<Node*>*)&obj), index, tid, (Node*) node);
    }

    inline Node* readNode(std::atomic<Node*>& obj, int index, int tid, Node* node){
        return memory_tracker->read(obj, index, tid, node);
    }

    inline OpDesc* reserveOpDesc(OpDesc* obj, int index, int tid, OpDesc* node){
        memory_tracker->reserve_slot((Node*) obj, index, tid, (Node*) node);
        return obj;
    }

    inline Node* reserveNode(Node* obj, int index, int tid, Node* node){
        memory_tracker->reserve_slot(obj, index, tid, node);
        return obj;
    }

    void help(long long phase, const int TID)
    {
        for (int i = 0; i < maxThreads; i++) {
            // Try to validate the HP for OpDesc at most MAX_OPDESC_TRANS times
            OpDesc* desc = reserveOpDesc(state[i].load(), kHpODCurr, TID, nullptr);
            int it = 0;
            for (; it < maxThreads+1; it++) {
                if (desc == state[i].load()) break;
                desc = reserveOpDesc(state[i].load(), kHpODCurr, TID, nullptr);
            }
            if (it == maxThreads+1 && desc != state[i].load()) continue;
            if (desc->pending && desc->phase <= phase) {
            	if (desc->enqueue) {
            		help_enq(i, phase, TID);
            	} else {
            		help_deq(i, phase, TID);
            	}
            }
        }
    }


    /**
     * Progress Condition: wait-free bounded by maxThreads
     */
    long long maxPhase(const int TID) {
        long long maxPhase = -1;
        for (int i = 0; i < maxThreads; i++) {
            // Try to validate the HP for OpDesc at most MAX_OPDESC_TRANS times
            OpDesc* desc = reserveOpDesc(state[i].load(), kHpODCurr, TID, nullptr);
            int it = 0;
            for (; it < maxThreads+1; it++) {
                if (desc == state[i].load()) break;
                desc = reserveOpDesc(state[i].load(), kHpODCurr, TID, nullptr);
            }
            if (it == maxThreads+1 && desc != state[i].load()) continue;
            long long phase = desc->phase;
            if (phase > maxPhase) {
            	maxPhase = phase;
            }
        }
        return maxPhase;
    }


    bool isStillPending(int tid, long long ph, const int TID) {
        OpDesc* desc = reserveOpDesc(state[tid].load(), kHpODNext, TID, nullptr);
        int it = 0;
        for (; it < maxThreads+1; it++) {
            if (desc == state[tid].load()) break;
            desc = reserveOpDesc(state[tid].load(), kHpODNext, TID, nullptr);
        }
        if (it == maxThreads+1 && desc != state[tid].load()) return false;
        return desc->pending && desc->phase <= ph;
    }


    bool insert(K __key, V item, int TID) {
        collect_retired_size(memory_tracker->get_retired_cnt(TID), TID);
        memory_tracker->start_op(TID);
        // We better have consecutive thread ids, otherwise this will blow up
        long long phase = maxPhase(TID) + 1;
        state[TID].store(mkOpDesc(phase, true, true, mkNode(item, TID), TID));
        help(phase, TID);
        help_finish_enq(TID);
        OpDesc* desc = state[TID].load();
        for (int i = 0; i < maxThreads*2; i++) { // Is maxThreads+1 enough?
            if (desc == OPDESC_END) break;
            if (state[TID].compare_exchange_strong(desc, OPDESC_END)) break;
            desc = state[TID].load();
        }
        memory_tracker->retire((Node*)desc, TID);
        memory_tracker->end_op(TID);
        memory_tracker->clear_all(TID);
        return true;
    }


    void help_enq(int tid, long long phase, const int TID) {
        while (isStillPending(tid, phase, TID)) {
            Node* last = reserveNode(tail.load(), kHpCurr, TID, nullptr);
            if (last != tail.load()) continue;
            Node* next = last->next.load();
            if (last == tail) {
                if (next == nullptr) {
                    if (isStillPending(tid, phase, TID)) {
                        OpDesc* curDesc = reserveOpDesc(state[tid].load(), kHpODCurr, TID, nullptr);
                        if (curDesc != state[tid].load()) continue;
                        if (last->casNext(next, curDesc->node)) {
                            help_finish_enq(TID);
                            return;
                        }
                    }
                } else {
                    help_finish_enq(TID);
                }
            }
        }
    }


    void help_finish_enq(const int TID) {
        Node* last = reserveNode(tail.load(), kHpCurr, TID, nullptr);
        if (last != tail.load()) return;
        // The inner loop will run at most twice, because last->next is immutable when non-null
        Node* next = readNode(last->next, kHpNext, TID, last);
        // Check "last" equals "tail" to prevent ABA on "last->next"
        if (last == tail && next != nullptr) {
            int tid = next->enqTid;
            OpDesc* curDesc = reserveOpDesc(state[tid].load(), kHpODCurr, TID, nullptr);
            if (curDesc != state[tid].load()) return;
            if (last == tail && curDesc->node == next) {
            	OpDesc* newDesc = mkOpDesc(curDesc->phase, false, true, next, TID);
            	OpDesc* tmp = curDesc;
            	if(state[tid].compare_exchange_strong(tmp, newDesc)){
            		memory_tracker->retire((Node*)curDesc, TID);
            	} else {
            		memory_tracker->reclaim((Node *)newDesc);
            	}
            	casTail(last, next);
            }
        }
    }


    optional<V> remove(K __key, int TID) {
        optional<V> value={};
        collect_retired_size(memory_tracker->get_retired_cnt(TID), TID);
        memory_tracker->start_op(TID);
        // We better have consecutive thread ids, otherwise this will blow up
        long long phase = maxPhase(TID) + 1;
        state[TID].store(mkOpDesc(phase, true, false, nullptr, TID));
        help(phase, TID);
        help_finish_deq(TID);
        OpDesc* curDesc = readOpDesc(state[TID], kHpODCurr, TID, nullptr);
        Node* node = curDesc->node; // No need for hp because this thread will be the one to retire "node"
        if (node == nullptr) {
            OpDesc* desc = state[TID].load();
            for (int i = 0; i < MAX_THREADS; i++) {
                if (state[TID].compare_exchange_strong(desc, OPDESC_END)) break;
                desc = state[TID].load();
                if (desc == OPDESC_END) break;
            }
            memory_tracker->retire((Node*)desc, TID);
            memory_tracker->end_op(TID);
            memory_tracker->clear_all(TID);
            return value; // We return null instead of throwing an exception
        }
        Node* next = node->next; // No need for chp because "next" can only be deleted when item set to nullptr
        value = next->item;
        next->itemDeletable.store(1); // "next" can be deleted now
        memory_tracker->retire(node, TID); // "node" will be deleted only when node.item == nullptr
        OpDesc* desc = state[TID].load();
        for (int i = 0; i < maxThreads*2; i++) { // Is maxThreads+1 enough?
            if (desc == OPDESC_END) break;
            if (state[TID].compare_exchange_strong(desc, OPDESC_END)) break;
            desc = state[TID].load();
        }
        memory_tracker->retire((Node*)desc, TID);
        memory_tracker->end_op(TID);
        memory_tracker->clear_all(TID);
        return value;
    }


    void help_deq(int tid, long long phase, const int TID) {
        while (isStillPending(tid, phase, TID)) {
            Node* first = reserveNode(head.load(), kHpPrev, TID, nullptr);
            Node* last = reserveNode(tail.load(), kHpCurr, TID, nullptr);
            if (first != head.load() || last != tail.load()) continue;
            Node* next = first->next.load();
            if (first == head) {
            	if (first == last) {
            		if (next == nullptr) {
            			OpDesc* curDesc = reserveOpDesc(state[tid].load(), kHpODCurr, TID, nullptr);
            			if (curDesc != state[tid].load()) continue;
            			if (last == tail && isStillPending(tid, phase, TID)) {
            			    OpDesc* newDesc = mkOpDesc(curDesc->phase, false, false, nullptr, TID);
            			    OpDesc* tmp = curDesc;
                            if (state[tid].compare_exchange_strong(tmp, newDesc)) {
                                memory_tracker->retire((Node*)curDesc, TID);
                            } else {
                                memory_tracker->reclaim((Node*)newDesc);
                            }
            			}
                    } else {
                        help_finish_enq(TID);
                    }
                } else {
                    OpDesc* curDesc = reserveOpDesc(state[tid].load(), kHpODCurr, TID, nullptr);
                    if (curDesc != state[tid].load()) continue;
                    Node* node = curDesc->node;
                    if (!isStillPending(tid, phase, TID)) break;
                    if (first == head && node != first) {
                        OpDesc* newDesc = mkOpDesc(curDesc->phase, true, false, first, TID);
                        OpDesc* tmp = curDesc;
                        if (state[tid].compare_exchange_strong(tmp, newDesc)) {
                            memory_tracker->retire((Node*)curDesc, TID);
                        } else {
                            memory_tracker->reclaim((Node *)newDesc);
                            continue;
                        }
                    }
                    int tmp = -1;
                    first->deqTid.compare_exchange_strong(tmp, tid);
                    help_finish_deq(TID);
                }
            }
        }
    }


    void help_finish_deq(const int TID) {
        Node* first = reserveNode(head.load(), kHpPrev, TID, nullptr);
        if (first != head.load()) return;
        Node* next = first->next.load();
        int tid = first->deqTid.load();
        if (tid != -1) {
            OpDesc* curDesc = nullptr;
            for (int i = 0; i < MAX_THREADS; i++) {
                curDesc = reserveOpDesc(state[tid].load(), kHpODCurr, TID, nullptr);
                if (curDesc == state[tid].load()) break;
                if (i == MAX_THREADS-1) return; // If the opdesc has changed these many times, the operation must be complete
            }
            if (first == head && next != nullptr) {
            	OpDesc* newDesc = mkOpDesc(curDesc->phase, false, false, curDesc->node, TID);
            	OpDesc* tmp = curDesc;
            	if (state[tid].compare_exchange_strong(tmp, newDesc)) {
                    memory_tracker->retire((Node*)curDesc, TID);
            	} else {
                    memory_tracker->reclaim((Node *)newDesc);
                }
            	casHead(first, next);
            }
        }
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
class WFQueueFactory : public RideableFactory {
    WFQueue<K,V>* build(GlobalTestConfig* gtc) {
        // FIXME: better be aligned
        // it should still be OK as is;
        // extra __pad1, __pad2 fields take care of misalignments
        return new WFQueue<K,V>(gtc);
    }
};

#endif /* _WFQUEUE_H_ */
