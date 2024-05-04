package edu.berkeley.cs186.database.concurrency;

import java.util.Arrays;
import java.util.List;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement DONE
        LockType[] lockTypes = {a, b};
        LockType[] reversedLockTypes = {b, a};

        // false를 반환하는 조합. true를 반환하는 조합이 false를 반환하는 조합보다 훨씬 많아 이것과 비교하도록 함.
        List<LockType[]> nonCompatibleList = Arrays.asList(
                new LockType[]{LockType.S, LockType.X},
                new LockType[]{LockType.S, LockType.IX},
                new LockType[]{LockType.S, LockType.SIX},
                new LockType[]{LockType.X, LockType.X});

        // false를 반환하는 조합에 포함되어 있으면 false, true를 반환하는 조합에 포함되어 있으면 true.
        return !(containsPair(nonCompatibleList, lockTypes) || containsPair(nonCompatibleList, reversedLockTypes));
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement DONE
        LockType[] lockTypes = {parentLockType, childLockType};

        // false를 반환하는 조합.
        List<LockType[]> nonCompatibleList = Arrays.asList(
                new LockType[]{LockType.IS, LockType.X},
                new LockType[]{LockType.IS, LockType.IX},
                new LockType[]{LockType.IS, LockType.SIX});

        // false를 반환하는 조합에 포함되어 있거나 부모만 LockType이 NL인 조합은 false.
        // true를 반환하는 조합에 포함되어 있으면 true.
        return !(containsPair(nonCompatibleList, lockTypes) || isNLLeftButNotRight(parentLockType, childLockType));
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement DONE

        LockType[] lockTypes = {substitute, required};

        // false를 반환하는 조합.
        List<LockType[]> nonCompatibleList = Arrays.asList(
                new LockType[]{LockType.S, LockType.X},
                new LockType[]{LockType.IS, LockType.S},
                new LockType[]{LockType.IS, LockType.X},
                new LockType[]{LockType.IS, LockType.IX},
                new LockType[]{LockType.IX, LockType.S},
                new LockType[]{LockType.IX, LockType.X},
                new LockType[]{LockType.SIX, LockType.X});

        // false를 반환하는 조합에 포함되어 있거나 substitute만 LockType이 NL인 조합은 false.
        // true를 반환하는 조합에 포함되어 있으면 true.
        return !(containsPair(nonCompatibleList, lockTypes) || isNLLeftButNotRight(substitute, required));
    }

    private static boolean containsPair(List<LockType[]> lockTypesList, LockType[] lockTypes) {
        return lockTypesList.stream()
                .anyMatch(pair -> Arrays.equals(pair, lockTypes));
    }

    private static boolean isNLLeftButNotRight(LockType left, LockType right) {
        return left.equals(LockType.NL) && !right.equals(LockType.NL);
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

