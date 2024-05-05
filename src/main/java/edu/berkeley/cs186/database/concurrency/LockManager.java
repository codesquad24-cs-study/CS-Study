package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.stream.Collectors;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement DONE

            // 기존의 lock들이 except id를 가졌거나 호환 가능하다면 허용
            // 즉, except id를 가지지 않으면서 호환되지 않는 lock들이 locks안에 전혀 없으면 true, 하나라도 있으면 false.
            return locks.stream()
                    .noneMatch(lock -> lock.transactionNum != except && !LockType.compatible(lock.lockType, lockType));
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement DONE

            // 잠금이 호환된다고 가정하므로 바로 lock 부여
            long transactionNum = lock.transactionNum;
            for (Lock existedLock : locks) {
                if (existedLock.transactionNum == transactionNum) { // 이미 'lock'이 있는 경우 리소스에 대한 잠금을 업데이트
                    existedLock.lockType = lock.lockType;
                    updateLock(lock);
                    return;
                }
            }
            locks.add(lock);
            transactionLocks.put(transactionNum, Collections.singletonList(lock)); // 트랜잭션에 'lock' 잠금을 부여
        }

        private void updateLock(Lock lock) {
            List<Lock> updateLocks = transactionLocks.get(lock.transactionNum).stream()
                    .filter(existedLock -> existedLock.name.equals(lock.name)).collect(Collectors.toList());

            for (Lock existedLock : updateLocks) {
                existedLock.lockType = lock.lockType;
            }
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement DONE
            locks.remove(lock); // lock 해제
            long transactionNum = lock.transactionNum;
            transactionLocks.get(transactionNum).remove(lock);
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement DONE
            if (addFront) {
                waitingQueue.addFirst(request);
                return;
            }
            waitingQueue.addLast(request);
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            // TODO(proj4_part1): implement DONE
            for (LockRequest lockRequest : waitingQueue) {
                // 잠금 부여가 불가능하다면 중지
                if (checkCompatible(lockRequest.lock.lockType, lockRequest.lock.transactionNum)) {
                    return;
                }
                LockRequest first = waitingQueue.removeFirst(); // 대기큐에서 제일 첫번째 요청 꺼내기
                first.releasedLocks.forEach(this::releaseLock);
                grantOrUpdateLock(first.lock);
                first.transaction.unblock();
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement DONE
            // transaction이랑 transactionNum이 일치하는 lock이 없으면 NL 리턴
            return locks.stream()
                    .filter(lock -> lock.transactionNum == transaction)
                    .map(lock -> lock.lockType).findFirst()
                    .orElse(LockType.NL);
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement DONE

        boolean shouldBlock = false;
        synchronized (this) {
            LockType transactionLockType = getLockType(transaction, name);
            if (transactionLockType == lockType) {
                throw new DuplicateLockRequestException("잠금이 이미 transaction에 의해 보유되어 있고 해제되지 않았습니다.");
            }

            ResourceEntry resourceEntry = getResourceEntry(name);
            long transactionNum = transaction.getTransNum();
            // name에 대한 lockType 잠금을 획득
            Lock lock = new Lock(name, lockType, transactionNum);
            if (resourceEntry.checkCompatible(lockType, transactionNum)) {
                resourceEntry.grantOrUpdateLock(lock);

                // 트랜잭션이 보유하고 있는 releaseNames에 대한 모든 잠금을 해제
                for (ResourceName resourceName : releaseNames) {
                    // name에 대한 잠금 획득 시간은 변경하지 않는다.
                    if (resourceName.equals(name)) {
                        continue;
                    }
                    release(transaction, resourceName);
                    resourceEntry.processQueue();
                }
            } else { // 새 잠금이 리소스에 대한 다른 트랜잭션의 잠금과 호환되지 않으면
                shouldBlock = true; // 트랜잭션 차단
                List<Lock> releasedLocks = getReleasedLocks(releaseNames, transactionNum);
                LockRequest lockRequest = new LockRequest(transaction, lock, releasedLocks);

                // 요청을 리소스 대기열의 맨 앞에 배치
                resourceEntry.addToQueue(lockRequest, true);
                transaction.prepareBlock();
            }
        }

        if (shouldBlock) {
            transaction.block();
        }
    }

    private List<Lock> getReleasedLocks(List<ResourceName> releaseNames, long transactionNum) {
        List<Lock> releasedLocks = new ArrayList<>();
        for (ResourceName resourceName : releaseNames) {
            LockType locktype = getResourceEntry(resourceName).getTransactionLockType(transactionNum);
            releasedLocks.add(new Lock(resourceName, locktype, transactionNum));
        }
        return releasedLocks;
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement DONE
        synchronized (this) {
            ResourceEntry resourceEntry = getResourceEntry(name);
            long transactionNum = transaction.getTransNum();

            // transaction이 releaseNames에 있는 하나 이상의 이름에 대한 잠금을 유지하지 않는 경우
            if (resourceEntry.getTransactionLockType(transactionNum) == LockType.NL) {
                throw new NoLockHeldException("transaction은 releaseNames에서 하나 이상의 잠금을 유지해야 합니다.");
            }
            LockType lockType = resourceEntry.getTransactionLockType(transactionNum);
            Lock lock = new Lock(name, lockType, transactionNum);
            resourceEntry.releaseLock(lock); // 잠금 해제
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement DONE
        ResourceEntry resourceEntry = getResourceEntry(name);
        long transactionNum = transaction.getTransNum();
        return resourceEntry.getTransactionLockType(transactionNum); // transaction이 name에 대해 가지고 있는 잠금 유형을 반환
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
