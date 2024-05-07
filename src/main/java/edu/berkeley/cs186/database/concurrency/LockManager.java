package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

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
            // TODO(proj4_part1): implement Done

            // except와 transactionNum이 다른것만 filter하고 allMatch로 호환성 확인
            return locks.stream()
                .filter(lock -> lock.transactionNum != except)
                .allMatch(lock -> LockType.compatible(lock.lockType, lockType));
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement Done
            Long inputTransactionNum = lock.transactionNum;
            // update
            for (Lock resourceLock : locks) {
                if (resourceLock.transactionNum == inputTransactionNum) {
                    resourceLock.lockType = lock.lockType;
                    for (Lock transactionLock : transactionLocks.get(inputTransactionNum)) {
                        if (transactionLock.name.equals(lock.name)) {
                            transactionLock.lockType = lock.lockType;
                        }
                    }
                    return;
                }
            }

            // add : resource의 locks와 transactinoLock를 모두 업데이트
            locks.add(lock);
            transactionLocks.put(inputTransactionNum, new ArrayList<>());
            transactionLocks.get(inputTransactionNum).add(lock);
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement Done
            locks.remove(lock);
            transactionLocks.get(lock.transactionNum).remove(lock);
            processQueue();
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement Done
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();
            LockRequest currentRequest;
            // TODO(proj4_part1): implement Done
            while (requests.hasNext()) {
                currentRequest = requests.next();
                if (checkCompatible(currentRequest.lock.lockType,
                    currentRequest.transaction.getTransNum())) {
                    waitingQueue.removeFirst();
                    grantOrUpdateLock(currentRequest.lock);
                    currentRequest.transaction.unblock();
                } else {
                    return;
                }
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement Done
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
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
        // TODO(proj4_part1): implement Done
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean isBlock = false;
        long transNum = transaction.getTransNum();
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            if (entry.getTransactionLockType(transNum) == lockType) {
                throw new DuplicateLockRequestException(
                    String.format("트랜잭션 %s가 이미 %s을 %s에 hold", transNum, lockType, name));
            }
            Lock lock = new Lock(name, lockType, transNum);
            if (!entry.checkCompatible(lockType, transNum)) {
                // prepare releasedLocks List
                List<Lock> releasedLocks = new ArrayList<>();
                for (ResourceName resourceName : releaseNames) {
                    LockType locktype = getResourceEntry(resourceName).getTransactionLockType(transNum);
                    releasedLocks.add(new Lock(name, locktype, transNum));
                }
                // releasedLocks를 생성자 파라미터에 넣어서 LockRequest 객체 생성
                LockRequest request = new LockRequest(transaction, lock, releasedLocks);
                // 큐 맨앞으로 보내기
                entry.addToQueue(request, true);
                // block 준비
                transaction.prepareBlock();
                // block
                isBlock = true;
            } else {
                // lock acquire
                entry.grantOrUpdateLock(lock);
                // lock release 0개 이상
                for (ResourceName resourceName : releaseNames) {
                    // 업데이트한 lock 제외
                    if (resourceName.equals(name)) continue;
                    // 없는 lock인지 확인
                    if (getResourceEntry(resourceName).getTransactionLockType(transNum) == LockType.NL) {
                        throw new NoLockHeldException(
                            String.format("트랜잭션 %s이 %s에 lock을 가지고 있지 않습니다.", transNum, name));
                    }
                    // release
                    release(transaction, resourceName);
                }
            }
        }
        if (isBlock) {
            transaction.block();
        }
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
        // TODO(proj4_part1): implement DOne
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean isBlock = false;
        long transNum = transaction.getTransNum();
        synchronized (this) {
            ResourceEntry resourceEntry = getResourceEntry(name);
            // resouceEntry에 이미 해당 transaction이 lock을 가지고 있는 경우
            if (resourceEntry.getTransactionLockType(transNum) != LockType.NL) {
                throw new DuplicateLockRequestException(
                    String.format("트랜잭션 %s가 이미 %s을 %s에 hold", transNum, lockType, name));
            }
            Lock lock = new Lock(name, lockType, transNum);
            // lockType이 compatible하지 않거나 대기중인 transaction이 있을 경우
            if (!resourceEntry.checkCompatible(lockType, transNum) || !resourceEntry.waitingQueue.isEmpty()) {
                // release 하지 않고 LockRequest 객체 생성
                LockRequest lockRequest = new LockRequest(transaction, lock);
                // queue 맨 뒤에 추가하기
                resourceEntry.addToQueue(lockRequest, false);
                // block 하기 전에 prepareBlock
                transaction.prepareBlock();
                // block으로 상태 변화
                isBlock = true;
            } else {
                // lockType이 compatible하고 대기중인 transaction이 없을 경우 acquire 한다
                resourceEntry.grantOrUpdateLock(lock);
            }
        }
        // blockdms synchronize 밖에서 처리
        if (isBlock) {
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
        // TODO(proj4_part1): implement Done
        // You may modify any part of this method.
        long transNum = transaction.getTransNum();
        synchronized (this) {
            ResourceEntry resourceEntry = getResourceEntry(name);
            LockType lockType = resourceEntry.getTransactionLockType(transNum);
            // 해당 transaction이 lock을 가지고 있지 않은 경우 예외처리
            if (lockType.equals(LockType.NL)) {
                throw new NoLockHeldException(
                    String.format("트랜잭션 %s이 %s에 lock을 가지고 있지 않습니다.", transNum, name));
            }
            Lock lock = new Lock(name, lockType, transNum);
            // releaseLock으로 lock을 해제하고 queue를 처리
            resourceEntry.releaseLock(lock);
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
        // TODO(proj4_part1): implement Done
        // You may modify any part of this method.
        long transNum = transaction.getTransNum();
        synchronized (this) {
            ResourceEntry resourceEntry = getResourceEntry(name);
            LockType prevLockType = resourceEntry.getTransactionLockType(transNum);

            // 해당 transaction이 lock을 가지고 있지 않은 경우 예외처리
            if (prevLockType.equals(LockType.NL)) {
                throw new NoLockHeldException(String.format("트랜잭션 %s이 %s에 lock을 가지고 있지 않습니다.", transNum, name));
            }
            // 이미 newLockType을 가지고 있는 경우 예외처리
            if (prevLockType.equals(newLockType)) {
                throw new DuplicateLockRequestException(
                    String.format("트랜잭션 %s가 이미 %s을 %s에 hold", transNum, newLockType, name));
            }
            // substitutable하지 않은 경우 예외처리
            if (!LockType.substitutable(prevLockType, newLockType)) {
                throw new InvalidLockException(
                    String.format("새로운 %slock이 %s lock을 대체할 수 없다 ", newLockType, prevLockType));
            }
            Lock lock = new Lock(name, newLockType, transNum);
            if (resourceEntry.checkCompatible(newLockType, transNum)) {
                resourceEntry.grantOrUpdateLock(lock);
                return;
            }
            LockRequest lockRequest = new LockRequest(transaction, lock);
            resourceEntry.addToQueue(lockRequest, false);
            transaction.prepareBlock();
        }
        transaction.block();
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement Done
        ResourceEntry resourceEntry = getResourceEntry(name);
        return resourceEntry.getTransactionLockType(transaction.getTransNum());
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
