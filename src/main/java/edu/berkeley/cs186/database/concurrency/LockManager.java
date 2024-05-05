package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager는 어떤 트랜잭션에 어떤 리소스에 어떤 잠금이 있는지 장부를 관리하고 큐 로직을 처리합니다.
 * 일반적으로 잠금 관리자는 직접 사용해서는 안 되며, 대신 코드에서 LockContext의 메서드를 호출하여 잠금을 획득/해제/승격/확대해야 합니다.
 *
 * LockManager는 주로 트랜잭션, 리소스, 잠금 간의 매핑에 관여하며, 여러 수준의 세분성에는 관여하지 않습니다. 다중 세분성은 LockContext가 대신 처리합니다.
 *
 * 잠금 관리자가 관리하는 각 리소스에는 당시 충족할 수 없었던 잠금 획득(또는 승격/획득 및 해제) 요청을 나타내는 자체 LockRequest 객체 큐가 있습니다.
 * 이 대기열은 해당 리소스에 대한 잠금이 해제될 때마다 첫 번째 요청부터 시작하여 요청을 충족할 수 없을 때까지 순서대로 처리되어야 합니다.
 *
 * 큐에서 제거된 요청은 큐가 없을 때 해당 트랜잭션이 자원이 해제된 직후에 요청을 한 것처럼 처리되어야 합니다
 * (즉, T1이 X(db)를 획득하기 위해 요청을 제거하면 T1이 방금 X(db)를 요청했고 큐에 큐가 없는 것처럼 처리되어야 합니다):
 * T1은 db에 X 잠금을 부여하고 Transaction#unblock을 통해 차단 해제 상태에 놓여야 합니다.)
 *
 * This does mean that in the case of:
 * queue: S(A) X(A) S(A)
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
            for (Lock lock : locks) {
                if (lock.transactionNum == except) continue;
                if (!LockType.compatible(lockType, lock.lockType)) return false;
            }

            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement Done
            Optional<Lock> lockByTransNum = findLockByTransNum(lock.transactionNum);

            if (lockByTransNum.isPresent()) {
                lockByTransNum.get().lockType = lock.lockType;
                updateLockToTrans(lock);
                return;
            }

            locks.add(lock);
            addLockToTrans(lock);
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement Done
            locks.remove(lock);
            removeLockFromTrans(lock);

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
            // TODO(proj4_part1): implement Done
            for (LockRequest request : waitingQueue) {
                if (checkCompatible(request.lock.lockType, request.transaction.getTransNum())) {
                    waitingQueue.remove(request);

                    grantOrUpdateLock(request.lock);

                    request.releasedLocks.forEach(lock -> release(request.transaction, lock.name));
                    request.transaction.unblock();
                } else return;
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement Done
            Optional<Lock> findByTransNum = findLockByTransNum(transaction);
            if (findByTransNum.isPresent()) return findByTransNum.get().lockType;
            return LockType.NL;
        }

        private Optional<Lock> findLockByTransNum(long transaction) {
            return locks.stream().filter(lock -> lock.transactionNum == transaction).findAny();
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

    private List<Lock> getTransactionLock(long transNum) {
        transactionLocks.putIfAbsent(transNum, new ArrayList<>());
        return transactionLocks.get(transNum);
    }

    private void addLockToTrans(Lock lock) {
        long transNum = lock.transactionNum;
        getTransactionLock(transNum).add(lock);
    }

    private void updateLockToTrans(Lock lock) {
        long transNum = lock.transactionNum;
        List<Lock> locks = transactionLocks.get(transNum);
        Optional<Lock> foundLock = locks.stream().filter(transLock -> transLock.name.equals(lock.name)).findAny();
        foundLock.ifPresent(fLock -> fLock.lockType = lock.lockType);
    }

    private void removeLockFromTrans(Lock lock) {
        long transNum = lock.transactionNum;
        transactionLocks.get(transNum).remove(lock);
    }

    private LockType getEntityLockType(ResourceName name, long transNum) {
        return getResourceEntry(name).getTransactionLockType(transNum);
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
     *                                       by `transaction` and isn't being released
     * @throws NoLockHeldException           if `transaction` doesn't hold a lock on one
     *                                       or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement

        boolean shouldBlock = false;
        synchronized (this) {
            long transNum = transaction.getTransNum();
            ResourceEntry resourceEntry = getResourceEntry(name);
            if (resourceEntry.getTransactionLockType(transNum) == lockType) {
                throw new DuplicateLockRequestException(name.toString());
            }

            Lock lock = new Lock(name, lockType, transNum);
            if (!resourceEntry.checkCompatible(lockType, transNum)) {
                shouldBlock = true;

                List<Lock> releasedLocks = new ArrayList<>();
                releaseNames.forEach(releaseName -> {
                    releasedLocks.add(new Lock(name, getEntityLockType(name, transNum), transNum));
                });

                LockRequest request = new LockRequest(transaction, lock, releasedLocks);
                resourceEntry.addToQueue(request, true);

                // To block a transaction, call Transaction#prepareBlock inside the synchronized block
                transaction.prepareBlock();
            } else {
                resourceEntry.grantOrUpdateLock(lock);

                for (ResourceName resourceName : releaseNames) {
                    if (resourceName.equals(name)) continue;
                    release(transaction, resourceName);
                }
            }
        }
        // call Transaction#block outside the synchronized block.
        if (shouldBlock) {
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
     *                                       `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement Done

        boolean shouldBlock = false;
        long transNum = transaction.getTransNum();
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            if (entry.getTransactionLockType(transNum) != LockType.NL)
                throw new DuplicateLockRequestException(name.toString());

            Lock lock = new Lock(name, lockType, transNum);
            if (!entry.waitingQueue.isEmpty() || !entry.checkCompatible(lockType, transNum)) {
                shouldBlock = true;
                LockRequest request = new LockRequest(transaction, lock);
                entry.addToQueue(request, false);
                transaction.prepareBlock();
            } else {
                entry.grantOrUpdateLock(lock);
            }
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
        // TODO(proj4_part1): implement Done

        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            LockType existingLockType = getLockType(transaction, name);
            long transNum = transaction.getTransNum();

            if (existingLockType == LockType.NL) throw new NoLockHeldException(name.toString());

            Lock lock = new Lock(name, existingLockType, transNum);
            entry.releaseLock(lock);
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
     *                                       `newLockType` lock on `name`
     * @throws NoLockHeldException           if `transaction` has no lock on `name`
     * @throws InvalidLockException          if the requested lock type is not a
     *                                       promotion. A promotion from lock type A to lock type B is valid if and
     *                                       only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement Done

        boolean shouldBlock = false;
        long transNum = transaction.getTransNum();
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            LockType existingLockType = entry.getTransactionLockType(transNum);

            // Error checking
            if (existingLockType == newLockType) throw new DuplicateLockRequestException(name.toString());
            if (existingLockType == LockType.NL) throw new NoLockHeldException(name.toString());
            if (!LockType.substitutable(newLockType, existingLockType)) throw new InvalidLockException(name.toString());

            Lock lock = new Lock(name, newLockType, transNum);
            if (!entry.checkCompatible(newLockType, transNum)) {
                shouldBlock = true;
                entry.addToQueue(new LockRequest(transaction, lock), true);
                transaction.prepareBlock();
            } else {
                entry.grantOrUpdateLock(lock);
            }
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
