package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.categories.Proj2Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.MemoryDiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.recovery.DummyRecoveryManager;
import edu.berkeley.cs186.database.table.RecordId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

import static org.junit.Assert.*;

@Category(Proj2Tests.class)
public class TestBPlusTree {
    private BufferManager bufferManager;
    private BPlusTreeMetadata metadata;
    private LockContext treeContext;

    // max 5 I/Os per iterator creation default
    private static final int MAX_IO_PER_ITER_CREATE = 5;

    // max 1 I/Os per iterator next, unless overridden
    private static final int MAX_IO_PER_NEXT = 1;

    // 3 seconds max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
            3000 * TimeoutScaling.factor)));

    @Before
    public void setup() {
        DiskSpaceManager diskSpaceManager = new MemoryDiskSpaceManager();
        diskSpaceManager.allocPart(0);
        this.bufferManager = new BufferManager(diskSpaceManager, new DummyRecoveryManager(), 1024,
                new ClockEvictionPolicy());
        this.treeContext = new DummyLockContext();
        this.metadata = null;
    }

    @After
    public void cleanup() {
        // If you run into errors with this line, try commenting it out and
        // seeing if any other errors appear which may be preventing
        // certain pages from being unpinned correctly.
        this.bufferManager.close();
    }

    // Helpers /////////////////////////////////////////////////////////////////
    private void setBPlusTreeMetadata(Type keySchema, int order) {
        this.metadata = new BPlusTreeMetadata("test", "col", keySchema, order,
                0, DiskSpaceManager.INVALID_PAGE_NUM, -1);
    }

    private BPlusTree getBPlusTree(Type keySchema, int order) {
        setBPlusTreeMetadata(keySchema, order);
        return new BPlusTree(bufferManager, metadata, treeContext);
    }

    // the 0th item in maxIOsOverride specifies how many I/Os constructing the iterator may take
    // the i+1th item in maxIOsOverride specifies how many I/Os the ith call to next() may take
    // if there are more items in the iterator than maxIOsOverride, then we default to
    // MAX_IO_PER_ITER_CREATE/MAX_IO_PER_NEXT once we run out of items in maxIOsOverride
    private <T> List<T> indexIteratorToList(Supplier<Iterator<T>> iteratorSupplier,
                                            Iterator<Integer> maxIOsOverride) {
        bufferManager.evictAll();

        long initialIOs = bufferManager.getNumIOs();

        long prevIOs = initialIOs;
        Iterator<T> iter = iteratorSupplier.get();
        long newIOs = bufferManager.getNumIOs();
        long maxIOs = maxIOsOverride.hasNext() ? maxIOsOverride.next() : MAX_IO_PER_ITER_CREATE;
        assertFalse("too many I/Os used constructing iterator (" + (newIOs - prevIOs) + " > " + maxIOs +
                        ") - are you materializing more than you need?",
                newIOs - prevIOs > maxIOs);

        List<T> xs = new ArrayList<>();
        while (iter.hasNext()) {
            prevIOs = bufferManager.getNumIOs();
            xs.add(iter.next());
            newIOs = bufferManager.getNumIOs();
            maxIOs = maxIOsOverride.hasNext() ? maxIOsOverride.next() : MAX_IO_PER_NEXT;
            assertFalse("too many I/Os used per next() call (" + (newIOs - prevIOs) + " > " + maxIOs +
                            ") - are you materializing more than you need?",
                    newIOs - prevIOs > maxIOs);
        }

        long finalIOs = bufferManager.getNumIOs();
        maxIOs = xs.size() / (2 * metadata.getOrder());
        assertTrue("too few I/Os used overall (" + (finalIOs - initialIOs) + " < " + maxIOs +
                        ") - are you materializing before the iterator is even constructed?",
                (finalIOs - initialIOs) >= maxIOs);
        return xs;
    }

    private <T> List<T> indexIteratorToList(Supplier<Iterator<T>> iteratorSupplier) {
        return indexIteratorToList(iteratorSupplier, Collections.emptyIterator());
    }

    // Tests ///////////////////////////////////////////////////////////////////

    @Test
    @Category(PublicTests.class)
    public void testSimpleBulkLoad() {
        // Creates a B+ Tree with order 2, fillFactor 0.75 and attempts to bulk
        // load 11 values.

        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        float fillFactor = 0.75f;
        assertEquals("()", tree.toSexp());

        List<Pair<DataBox, RecordId>> data = new ArrayList<>();
        for (int i = 1; i <= 11; ++i) {
            data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
        }

        tree.bulkLoad(data.iterator(), fillFactor);
        //      (    4        7         10        _   )
        //       /       |         |         \
        // (1 2 3 _) (4 5 6 _) (7 8 9 _) (10 11 _ _)
        String leaf0 = "((1 (1 1)) (2 (2 2)) (3 (3 3)))";
        String leaf1 = "((4 (4 4)) (5 (5 5)) (6 (6 6)))";
        String leaf2 = "((7 (7 7)) (8 (8 8)) (9 (9 9)))";
        String leaf3 = "((10 (10 10)) (11 (11 11)))";
        String sexp = String.format("(%s 4 %s 7 %s 10 %s)", leaf0, leaf1, leaf2, leaf3);
        assertEquals(sexp, tree.toSexp());
    }

    @Test
    @Category(PublicTests.class)
    public void testWhiteBoxTest() {
        // This test will insert values one by one into your B+ tree implementation.
        // We've provided a visualization of how your tree should be structured
        // after each step.
        BPlusTree tree = getBPlusTree(Type.intType(), 1);
        assertEquals("()", tree.toSexp());

        // (4)
        tree.put(new IntDataBox(4), new RecordId(4, (short) 4));
        assertEquals("((4 (4 4)))", tree.toSexp());

        // (4 9)
        tree.put(new IntDataBox(9), new RecordId(9, (short) 9));
        assertEquals("((4 (4 4)) (9 (9 9)))", tree.toSexp());

        //   (6)
        //  /   \
        // (4) (6 9)
        tree.put(new IntDataBox(6), new RecordId(6, (short) 6));
        String l = "((4 (4 4)))";
        String r = "((6 (6 6)) (9 (9 9)))";
        assertEquals(String.format("(%s 6 %s)", l, r), tree.toSexp());

        //     (6)
        //    /   \
        // (2 4) (6 9)
        tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
        l = "((2 (2 2)) (4 (4 4)))";
        r = "((6 (6 6)) (9 (9 9)))";
        assertEquals(String.format("(%s 6 %s)", l, r), tree.toSexp());

        //      (6 7)
        //     /  |  \
        // (2 4) (6) (7 9)
        tree.put(new IntDataBox(7), new RecordId(7, (short) 7));
        l = "((2 (2 2)) (4 (4 4)))";
        String m = "((6 (6 6)))";
        r = "((7 (7 7)) (9 (9 9)))";
        assertEquals(String.format("(%s 6 %s 7 %s)", l, m, r), tree.toSexp());

        //         (7)
        //        /   \
        //     (6)     (8)
        //    /   \   /   \
        // (2 4) (6) (7) (8 9)
        tree.put(new IntDataBox(8), new RecordId(8, (short) 8));
        String ll = "((2 (2 2)) (4 (4 4)))";
        String lr = "((6 (6 6)))";
        String rl = "((7 (7 7)))";
        String rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 6 %s)", ll, lr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 7 %s)", l, r), tree.toSexp());

        //            (7)
        //           /   \
        //     (3 6)       (8)
        //   /   |   \    /   \
        // (2) (3 4) (6) (7) (8 9)
        tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
        ll = "((2 (2 2)))";
        String lm = "((3 (3 3)) (4 (4 4)))";
        lr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 3 %s 6 %s)", ll, lm, lr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 7 %s)", l, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //   (3)      (6)       (8)
        //  /   \    /   \    /   \
        // (2) (3) (4 5) (6) (7) (8 9)
        tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        String ml = "((4 (4 4)) (5 (5 5)))";
        String mr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (1 2) (3) (4 5) (6) (7) (8 9)
        tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
        ll = "((1 (1 1)) (2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((4 (4 4)) (5 (5 5)))";
        mr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (  2) (3) (4 5) (6) (7) (8 9)
        tree.remove(new IntDataBox(1));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((4 (4 4)) (5 (5 5)))";
        mr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (  2) (3) (4 5) (6) (7) (8  )
        tree.remove(new IntDataBox(9));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((4 (4 4)) (5 (5 5)))";
        mr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (  2) (3) (4 5) ( ) (7) (8  )
        tree.remove(new IntDataBox(6));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((4 (4 4)) (5 (5 5)))";
        mr = "()";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (  2) (3) (  5) ( ) (7) (8  )
        tree.remove(new IntDataBox(4));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((5 (5 5)))";
        mr = "()";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) (3) (  5) ( ) (7) (8  )
        tree.remove(new IntDataBox(2));
        ll = "()";
        lr = "((3 (3 3)))";
        ml = "((5 (5 5)))";
        mr = "()";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) (3) (   ) ( ) (7) (8  )
        tree.remove(new IntDataBox(5));
        ll = "()";
        lr = "((3 (3 3)))";
        ml = "()";
        mr = "()";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) (3) (   ) ( ) ( ) (8  )
        tree.remove(new IntDataBox(7));
        ll = "()";
        lr = "((3 (3 3)))";
        ml = "()";
        mr = "()";
        rl = "()";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) ( ) (   ) ( ) ( ) (8  )
        tree.remove(new IntDataBox(3));
        ll = "()";
        lr = "()";
        ml = "()";
        mr = "()";
        rl = "()";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) ( ) (   ) ( ) ( ) (   )
        tree.remove(new IntDataBox(8));
        ll = "()";
        lr = "()";
        ml = "()";
        mr = "()";
        rl = "()";
        rr = "()";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());
    }

    @Test
    @Category(PublicTests.class)
    public void testRandomPuts() {
        // This test will generate 1000 keys and for trees of degree 2, 3 and 4
        // will scramble the keys and attempt to insert them.
        //
        // After insertion we test scanAll and scanGreaterEqual to ensure all
        // the keys were inserted and could be retrieved in the proper order.
        //
        // Finally, we remove each of the keys one-by-one and check to see that
        // they can no longer be retrieved.

        List<DataBox> keys = new ArrayList<>();
        List<RecordId> rids = new ArrayList<>();
        List<RecordId> sortedRids = new ArrayList<>();
        for (int i = 0; i < 1000; ++i) {
            keys.add(new IntDataBox(i));
            rids.add(new RecordId(i, (short) i));
            sortedRids.add(new RecordId(i, (short) i));
        }

        // Try trees with different orders.
        for (int d = 2; d < 5; ++d) {
            // Try trees with different insertion orders.
            for (int n = 0; n < 2; ++n) {
                Collections.shuffle(keys, new Random(42));
                Collections.shuffle(rids, new Random(42));

                // Insert all the keys.
                BPlusTree tree = getBPlusTree(Type.intType(), d);
                for (int i = 0; i < keys.size(); ++i) {
                    tree.put(keys.get(i), rids.get(i));
                }

                // Test get.
                for (int i = 0; i < keys.size(); ++i) {
                    assertEquals(Optional.of(rids.get(i)), tree.get(keys.get(i)));
                }

                // Test scanAll.
                assertEquals(sortedRids, indexIteratorToList(tree::scanAll));

                // Test scanGreaterEqual.
                for (int i = 0; i < keys.size(); i += 100) {
                    final int j = i;
                    List<RecordId> expected = sortedRids.subList(i, sortedRids.size());
                    assertEquals(expected, indexIteratorToList(() -> tree.scanGreaterEqual(new IntDataBox(j))));
                }

                // Load the tree from disk.
                BPlusTree fromDisk = new BPlusTree(bufferManager, metadata, treeContext);
                assertEquals(sortedRids, indexIteratorToList(fromDisk::scanAll));

                // Test remove.
                Collections.shuffle(keys, new Random(42));
                Collections.shuffle(rids, new Random(42));
                for (DataBox key : keys) {
                    fromDisk.remove(key);
                    assertEquals(Optional.empty(), fromDisk.get(key));
                }
            }
        }
    }

    @Test
    @Category(SystemTests.class)
    public void testMaxOrder() {
        // Note that this white box test depend critically on the implementation
        // of toBytes and includes a lot of magic numbers that won't make sense
        // unless you read toBytes.
        assertEquals(4, Type.intType().getSizeInBytes());
        assertEquals(8, Type.longType().getSizeInBytes());
        assertEquals(10, RecordId.getSizeInBytes());
        short pageSizeInBytes = 100;
        Type keySchema = Type.intType();
        assertEquals(3, LeafNode.maxOrder(pageSizeInBytes, keySchema));
        assertEquals(3, InnerNode.maxOrder(pageSizeInBytes, keySchema));
        assertEquals(3, BPlusTree.maxOrder(pageSizeInBytes, keySchema));
    }

    @Test
    public void testDeleteEverything() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        for (int i = 0; i < 100; i++) {
            tree.put(new IntDataBox(i), new RecordId(i, (short) i));
        }

        Iterator<RecordId> iterBeforeDeleting = tree.scanAll();
        assertTrue(iterBeforeDeleting.hasNext());

        for (int i = 0; i < 100; i++) {
            tree.remove(new IntDataBox(i));
        }

        Iterator<RecordId> iterAfterDeleting = tree.scanAll();
        assertFalse(iterAfterDeleting.hasNext());
    }

    // Hidden /////////////////////////////////////////////////////////////////////

    @Test
    @Category(PublicTests.class)
    public void testEmptyTree() throws BPlusTreeException, IOException {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        List<RecordId> empty = new ArrayList<>();

        // Make sure that operations on an empty B+ tree doesn't throw any
        // exceptions.
        for (int i = 0; i < 10; ++i) {
            tree.remove(new IntDataBox(i));
            assertEquals(Optional.empty(), tree.get(new IntDataBox(i)));
            Iterator<RecordId> eq = tree.scanEqual(new IntDataBox(i));
            Iterator<RecordId> all = tree.scanAll();
            Iterator<RecordId> ge = tree.scanGreaterEqual(new IntDataBox(i));
            assertEquals(empty, indexIteratorToList(() -> eq));
            assertEquals(empty, indexIteratorToList(() -> all));
            assertEquals(empty, indexIteratorToList(() -> ge));
        }
    }

    // HIDDEN
    @Test
    public void testSimpleGets() throws BPlusTreeException, IOException {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        for (int i = 0; i < 100; ++i) {
            tree.put(new IntDataBox(i), new RecordId(i, (short) i));
        }

        for (int i = 0; i < 100; ++i) {
            IntDataBox key = new IntDataBox(i);
            RecordId rid = new RecordId(i, (short) i);
            assertEquals(Optional.of(rid), tree.get(key));
        }

        for (int i = 100; i < 150; ++i) {
            assertEquals(Optional.empty(), tree.get(new IntDataBox(i)));
        }
    }

    // HIDDEN
    @Test
    public void testEmptyScans() throws BPlusTreeException, IOException {
        // Create and then empty the tree.
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        for (int i = 0; i < 100; ++i) {
            tree.put(new IntDataBox(i), new RecordId(i, (short) i));
        }
        for (int i = 0; i < 100; ++i) {
            tree.remove(new IntDataBox(i));
        }

        // Scan over the tree.
        Iterator<RecordId> actual = tree.scanAll();
        Iterator<RecordId> finalActual2 = actual;
        assertEquals(new ArrayList<RecordId>(), indexIteratorToList(() -> finalActual2));
        actual = tree.scanGreaterEqual(new IntDataBox(42));
        Iterator<RecordId> finalActual1 = actual;
        assertEquals(new ArrayList<RecordId>(), indexIteratorToList(() -> finalActual1));
        actual = tree.scanGreaterEqual(new IntDataBox(100));
        Iterator<RecordId> finalActual = actual;
        assertEquals(new ArrayList<RecordId>(), indexIteratorToList(() -> finalActual));
    }

    // HIDDEN
    @Test
    public void testPartiallyEmptyScans()
            throws BPlusTreeException, IOException {
        // Create and then empty part of the tree.
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        for (int i = 0; i < 100; ++i) {
            tree.put(new IntDataBox(i), new RecordId(i, (short) i));
        }
        for (int i = 25; i < 75; ++i) {
            tree.remove(new IntDataBox(i));
        }

        // Scan over the tree.
        Iterator<RecordId> actual = tree.scanAll();
        List<RecordId> expected = new ArrayList<>();
        for (int i = 0; i < 25; ++i) {
            expected.add(new RecordId(i, (short) i));
        }
        for (int i = 75; i < 100; ++i) {
            expected.add(new RecordId(i, (short) i));
        }
        Iterator<RecordId> finalActual = actual;
        assertEquals(expected, indexIteratorToList(() -> finalActual));

        actual = tree.scanGreaterEqual(new IntDataBox(42));
        expected = new ArrayList<>();
        for (int i = 75; i < 100; ++i) {
            expected.add(new RecordId(i, (short) i));
        }
        Iterator<RecordId> finalActual1 = actual;
        assertEquals(expected, indexIteratorToList(() -> finalActual1));

        actual = tree.scanGreaterEqual(new IntDataBox(99));
        expected = new ArrayList<>();
        expected.add(new RecordId(99, (short) 99));
        Iterator<RecordId> finalActual2 = actual;
        assertEquals(expected, indexIteratorToList(() -> finalActual2));
    }

    @Test(expected = BPlusTreeException.class)
    public void testDuplicatePut() throws BPlusTreeException {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        tree.put(new IntDataBox(0), new RecordId(0, (short) 0));
        tree.put(new IntDataBox(0), new RecordId(0, (short) 0));
    }

    // HIDDEN
    @Test
    public void testRandomRids() throws BPlusTreeException {
      int d = 3;
      BPlusTree tree = getBPlusTree(Type.intType(), d);

      List<DataBox> keys = new ArrayList<DataBox>();
      List<RecordId> rids = new ArrayList<RecordId>();
      for (int i = 0; i < 50 * d; ++i) {
        keys.add(new IntDataBox(i));
        rids.add(new RecordId(i, (short) i));
      }
      Collections.shuffle(rids, new Random(42));

      for (int i = 0; i < keys.size(); ++i) {
        tree.put(keys.get(i), rids.get(i));
        assertEquals(Optional.of(rids.get(i)), tree.get(keys.get(i)));
      }

      for (int i = 0; i < keys.size(); ++i) {
        assertEquals(Optional.of(rids.get(i)), tree.get(keys.get(i)));
      }
    }

    // HIDDEN
    @Test
    public void testDeepBulkLoad1() throws BPlusTreeException, IOException {
      // order 1, ff 1.0, n=15
      BPlusTree tree = getBPlusTree(Type.intType(), 1);
      float fillFactor = 1.0f;
      assertEquals("()", tree.toSexp());

      List<Pair<DataBox, RecordId>> data = new ArrayList<>();
      for (int i = 1; i <= 15; ++i) {
        data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
      }

      tree.bulkLoad(data.iterator(), fillFactor);
      String leaf0 = "((1 (1 1)) (2 (2 2)))";
      String leaf1 = "((3 (3 3)) (4 (4 4)))";
      String leaf2 = "((5 (5 5)) (6 (6 6)))";
      String leaf3 = "((7 (7 7)) (8 (8 8)))";
      String leaf4 = "((9 (9 9)) (10 (10 10)))";
      String leaf5 = "((11 (11 11)) (12 (12 12)))";
      String leaf6 = "((13 (13 13)) (14 (14 14)))";
      String leaf7 = "((15 (15 15)))";
      String inner_0_0 = String.format("(%s 3 %s)", leaf0, leaf1);
      String inner_0_1 = String.format("(%s 7 %s)", leaf2, leaf3);
      String inner_0_2 = String.format("(%s 11 %s)", leaf4, leaf5);
      String inner_0_3 = String.format("(%s 15 %s)", leaf6, leaf7);
      String inner_1_0 = String.format("(%s 5 %s)", inner_0_0, inner_0_1);
      String inner_1_1 = String.format("(%s 13 %s)", inner_0_2, inner_0_3);
      String root = String.format("(%s 9 %s)", inner_1_0, inner_1_1);
      assertEquals(root, tree.toSexp());
    }

    // HIDDEN
    @Test
    public void testDeepBulkLoad2() throws BPlusTreeException, IOException {
      // order 1, ff 0.5001, n=31
      BPlusTree tree = getBPlusTree(Type.intType(), 1);
      float fillFactor = 0.5001f;
      assertEquals("()", tree.toSexp());

      List<Pair<DataBox, RecordId>> data = new ArrayList<>();
      for (int i = 1; i <= 31; ++i) {
        data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
      }

      tree.bulkLoad(data.iterator(), fillFactor);
      String[] leaf = {
          "((1 (1 1)) (2 (2 2)))",
          "((3 (3 3)) (4 (4 4)))",
          "((5 (5 5)) (6 (6 6)))",
          "((7 (7 7)) (8 (8 8)))",
          "((9 (9 9)) (10 (10 10)))",
          "((11 (11 11)) (12 (12 12)))",
          "((13 (13 13)) (14 (14 14)))",
          "((15 (15 15)) (16 (16 16)))",
          "((17 (17 17)) (18 (18 18)))",
          "((19 (19 19)) (20 (20 20)))",
          "((21 (21 21)) (22 (22 22)))",
          "((23 (23 23)) (24 (24 24)))",
          "((25 (25 25)) (26 (26 26)))",
          "((27 (27 27)) (28 (28 28)))",
          "((29 (29 29)) (30 (30 30)))",
          "((31 (31 31)))",
      };
      String[] inner_0 = {
          String.format("(%s 3 %s)", leaf[0], leaf[1]),
          String.format("(%s 7 %s)", leaf[2], leaf[3]),
          String.format("(%s 11 %s)", leaf[4], leaf[5]),
          String.format("(%s 15 %s)", leaf[6], leaf[7]),
          String.format("(%s 19 %s)", leaf[8], leaf[9]),
          String.format("(%s 23 %s)", leaf[10], leaf[11]),
          String.format("(%s 27 %s)", leaf[12], leaf[13]),
          String.format("(%s 31 %s)", leaf[14], leaf[15]),
      };
      String[] inner_1 = {
          String.format("(%s 5 %s)", inner_0[0], inner_0[1]),
          String.format("(%s 13 %s)", inner_0[2], inner_0[3]),
          String.format("(%s 21 %s)", inner_0[4], inner_0[5]),
          String.format("(%s 29 %s)", inner_0[6], inner_0[7]),
      };
      String[] inner_2 = {
          String.format("(%s 9 %s)", inner_1[0], inner_1[1]),
          String.format("(%s 25 %s)", inner_1[2], inner_1[3]),
      };
      String root = String.format("(%s 17 %s)", inner_2[0], inner_2[1]);
      assertEquals(root, tree.toSexp());
    }

    // HIDDEN
    @Test
    public void testBulkLoadEmpty() throws BPlusTreeException, IOException {
      // order 5, ff=0.8, n=0
      BPlusTree tree = getBPlusTree(Type.intType(), 4);
      float fillFactor = 0.8f;
      assertEquals("()", tree.toSexp());

      List<Pair<DataBox, RecordId>> data = new ArrayList<>();
      tree.bulkLoad(data.iterator(), fillFactor);
      assertEquals("()", tree.toSexp());
    }

    // HIDDEN
    @Test
    public void testBulkLoadPreSplit() throws BPlusTreeException, IOException {
      // order 2, ff 0.75, n=15
      BPlusTree tree = getBPlusTree(Type.intType(), 2);
      float fillFactor = 0.75f;
      assertEquals("()", tree.toSexp());

      List<Pair<DataBox, RecordId>> data = new ArrayList<>();
      for (int i = 1; i <= 15; ++i) {
        data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
      }

      tree.bulkLoad(data.iterator(), fillFactor);
      String[] leaf = {
          "((1 (1 1)) (2 (2 2)) (3 (3 3)))",
          "((4 (4 4)) (5 (5 5)) (6 (6 6)))",
          "((7 (7 7)) (8 (8 8)) (9 (9 9)))",
          "((10 (10 10)) (11 (11 11)) (12 (12 12)))",
          "((13 (13 13)) (14 (14 14)) (15 (15 15)))",
      };
      String root = String.format("(%s 4 %s 7 %s 10 %s 13 %s)", leaf[0], leaf[1], leaf[2], leaf[3], leaf[4]);
      assertEquals(root, tree.toSexp());
    }

    // HIDDEN
    @Test
    public void testBulkLoadPostSplit() throws BPlusTreeException, IOException {
      // order 2, ff 0.75, n=16
      BPlusTree tree = getBPlusTree(Type.intType(), 2);
      float fillFactor = 0.75f;
      assertEquals("()", tree.toSexp());

      List<Pair<DataBox, RecordId>> data = new ArrayList<>();
      for (int i = 1; i <= 16; ++i) {
        data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
      }

      tree.bulkLoad(data.iterator(), fillFactor);
      String[] leaf = {
          "((1 (1 1)) (2 (2 2)) (3 (3 3)))",
          "((4 (4 4)) (5 (5 5)) (6 (6 6)))",
          "((7 (7 7)) (8 (8 8)) (9 (9 9)))",
          "((10 (10 10)) (11 (11 11)) (12 (12 12)))",
          "((13 (13 13)) (14 (14 14)) (15 (15 15)))",
          "((16 (16 16)))",
      };
      String[] inner_0 = {
          String.format("(%s 4 %s 7 %s)", leaf[0], leaf[1], leaf[2]),
          String.format("(%s 13 %s 16 %s)", leaf[3], leaf[4], leaf[5]),
      };
      String root = String.format("(%s 10 %s)", inner_0[0], inner_0[1]);
      assertEquals(root, tree.toSexp());
    }

    // HIDDEN
    @Test
    public void testBulkLoadPreSplitSparse() throws BPlusTreeException, IOException {
      // order 3, ff 0.25, n=22
      BPlusTree tree = getBPlusTree(Type.intType(), 3);
      float fillFactor = 0.25f;
      assertEquals("()", tree.toSexp());

      List<Pair<DataBox, RecordId>> data = new ArrayList<>();
      for (int i = 1; i <= 22; ++i) {
        data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
      }

      tree.bulkLoad(data.iterator(), fillFactor);
      String[] leaf = {
          "((1 (1 1)) (2 (2 2)))",
          "((3 (3 3)) (4 (4 4)))",
          "((5 (5 5)) (6 (6 6)))",
          "((7 (7 7)) (8 (8 8)))",
          "((9 (9 9)) (10 (10 10)))",
          "((11 (11 11)) (12 (12 12)))",
          "((13 (13 13)) (14 (14 14)))",
          "((15 (15 15)) (16 (16 16)))",
          "((17 (17 17)) (18 (18 18)))",
          "((19 (19 19)) (20 (20 20)))",
          "((21 (21 21)) (22 (22 22)))",
      };
      String[] inner_0 = {
          String.format("(%s 3 %s 5 %s 7 %s)", leaf[0], leaf[1], leaf[2], leaf[3]),
          String.format("(%s 11 %s 13 %s 15 %s 17 %s 19 %s 21 %s)", leaf[4], leaf[5], leaf[6], leaf[7], leaf[8], leaf[9], leaf[10]),
      };
      String root = String.format("(%s 9 %s)", inner_0[0], inner_0[1]);
      assertEquals(root, tree.toSexp());
    }

    // HIDDEN
    @Test
    public void testBulkLoadPostSplitSparse() throws BPlusTreeException, IOException {
      // order 3, ff 0.25, n=23
      BPlusTree tree = getBPlusTree(Type.intType(), 3);
      float fillFactor = 0.25f;
      assertEquals("()", tree.toSexp());

      List<Pair<DataBox, RecordId>> data = new ArrayList<>();
      for (int i = 1; i <= 23; ++i) {
        data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
      }

      tree.bulkLoad(data.iterator(), fillFactor);
      String[] leaf = {
          "((1 (1 1)) (2 (2 2)))",
          "((3 (3 3)) (4 (4 4)))",
          "((5 (5 5)) (6 (6 6)))",
          "((7 (7 7)) (8 (8 8)))",
          "((9 (9 9)) (10 (10 10)))",
          "((11 (11 11)) (12 (12 12)))",
          "((13 (13 13)) (14 (14 14)))",
          "((15 (15 15)) (16 (16 16)))",
          "((17 (17 17)) (18 (18 18)))",
          "((19 (19 19)) (20 (20 20)))",
          "((21 (21 21)) (22 (22 22)))",
          "((23 (23 23)))",
      };
      String[] inner_0 = {
          String.format("(%s 3 %s 5 %s 7 %s)", leaf[0], leaf[1], leaf[2], leaf[3]),
          String.format("(%s 11 %s 13 %s 15 %s)", leaf[4], leaf[5], leaf[6], leaf[7]),
          String.format("(%s 19 %s 21 %s 23 %s)", leaf[8], leaf[9], leaf[10], leaf[11]),
      };
      String root = String.format("(%s 9 %s 17 %s)", inner_0[0], inner_0[1], inner_0[2]);
      assertEquals(root, tree.toSexp());
    }

    // HIDDEN
    @Test
    public void testRepeatedInsertsAndRemoves()
        throws BPlusTreeException, IOException {
      BPlusTree tree = getBPlusTree(Type.intType(), 4);

      // Insert [0, 200).
      for (int i = 0; i < 200; ++i) {
        tree.put(new IntDataBox(i), new RecordId(i, (short) i));
      }

      // Delete [100, 200).
      for (int i = 100; i < 200; ++i) {
        tree.remove(new IntDataBox(i));
      }

      // Insert [150, 300).
      for (int i = 150; i < 300; ++i) {
        tree.put(new IntDataBox(i), new RecordId(i, (short) i));
      }

      // Delete [250, 300).
      for (int i = 250; i < 300; ++i) {
        tree.remove(new IntDataBox(i));
      }

      // Add [100, 150]
      for (int i = 100; i < 150; ++i) {
        tree.put(new IntDataBox(i), new RecordId(i, (short) i));
      }

      // Add [250, 300]
      for (int i = 250; i < 300; ++i) {
        tree.put(new IntDataBox(i), new RecordId(i, (short) i));
      }

      // Range [0, 300) should be full.
      List<RecordId> rids = new ArrayList<>();
      for (int i = 0; i < 300; ++i) {
        rids.add(new RecordId(i, (short) i));
      }
      assertEquals(rids, indexIteratorToList(tree::scanAll));
    }
}
