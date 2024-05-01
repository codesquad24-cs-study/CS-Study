package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.HashFunc;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.disk.Partition;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.*;

public class GHJOperator extends JoinOperator {
    private int numBuffers;
    private Run joinedRecords;

    public GHJOperator(QueryOperator leftSource,
                       QueryOperator rightSource,
                       String leftColumnName,
                       String rightColumnName,
                       TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.GHJ);
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
        this.joinedRecords = null;
    }

    @Override
    public int estimateIOCost() {
        // Since this has a chance of failing on certain inputs we give it the
        // maximum possible cost to encourage the optimizer to avoid it
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean materialized() {
        return true;
    }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (joinedRecords == null) {
            // Executing GHJ on-the-fly is arduous without coroutines, so
            // instead we'll accumulate all of our joined records in this run
            // and return an iterator over it once the algorithm completes
            this.joinedRecords = new Run(getTransaction(), getSchema());
            this.run(getLeftSource(), getRightSource(), 1);
        }
        ;
        return joinedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * For every record in the given iterator, hashes the value at the column we're joining on and adds it to the
     * correct partition in partitions.
     *
     * @param partitions an array of partitions to split the records into
     * @param records    iterable of records we want to partition
     * @param left       true if records are from the left relation, otherwise false
     * @param pass       the current pass (used to pick a hash function)
     */
    private void partition(Partition[] partitions, Iterable<Record> records, boolean left, int pass) {
        // TODO(proj3_part1): implement the partitioning logic DONE

        for (Record record : records) {
            // 지정된 반복자의 모든 레코드에 대해 조인 중인 열의 값을 해시
            DataBox columnValue = getColumnValue(left, record);
            int hashDataBox = HashFunc.hashDataBox(columnValue, pass);

            int partitionNum = hashDataBox % partitions.length;
            if (partitionNum < 0) {
                partitionNum += partitions.length;
            }
            partitions[partitionNum].add(record);
        }
    }

    private DataBox getColumnValue(boolean left, Record record) {
        if (left) {
            return record.getValue(getLeftColumnIndex());
        }
        return record.getValue(getRightColumnIndex());
    }

    /**
     * Runs the buildAndProbe stage on a given partition. Should add any matching records found during the probing stage
     * to this.joinedRecords.
     */
    private void buildAndProbe(Partition leftPartition, Partition rightPartition) {
        // true if the probe records come from the left partition, false otherwise
        boolean probeFirst;
        // We'll build our in memory hash table with these records
        Iterable<Record> buildRecords;
        // We'll probe the table with these records
        Iterable<Record> probeRecords;
        // The index of the join column for the build records
        int buildColumnIndex;
        // The index of the join column for the probe records
        int probeColumnIndex;

        if (leftPartition.getNumPages() <= this.numBuffers - 2) {
            buildRecords = leftPartition;
            buildColumnIndex = getLeftColumnIndex();
            probeRecords = rightPartition;
            probeColumnIndex = getRightColumnIndex();
            probeFirst = false;
        } else if (rightPartition.getNumPages() <= this.numBuffers - 2) {
            buildRecords = rightPartition;
            buildColumnIndex = getRightColumnIndex();
            probeRecords = leftPartition;
            probeColumnIndex = getLeftColumnIndex();
            probeFirst = true;
        } else {
            throw new IllegalArgumentException(
                    "Neither the left nor the right records in this partition " +
                            "fit in B-2 pages of memory."
            );
        }
        /**
         * 지정된 파티션에서 buildAndProbe 단계를 실행합니다.
         * 검색 단계에서 발견된 일치하는 레코드를 this.joinedRecords에 추가해야 합니다
         */
        // TODO(proj3_part1): implement the building and probing stage DONE

        Map<DataBox, List<Record>> hashTable = new HashMap<>();
        // Building stage
        for (Record buildRecord : buildRecords) {
            DataBox value = buildRecord.getValue(buildColumnIndex);
            if (!hashTable.containsKey(value)) {
                hashTable.put(value, new ArrayList<>());
            }
            hashTable.get(value).add(buildRecord);
        }

        // Probing stage
        for (Record probeRecord : probeRecords) {
            DataBox value = probeRecord.getValue(probeColumnIndex);
            if (!hashTable.containsKey(value)) {
                continue;
            }

            for (Record hashRecord : hashTable.get(value)) {
                Record joinedRecord = getJoinValue(probeFirst, probeRecord, hashRecord);
                this.joinedRecords.add(joinedRecord);
            }
        }
    }

    private Record getJoinValue(boolean probeFirst, Record probeRecord, Record hashRecord) {
        if (probeFirst) {
            return probeRecord.concat(hashRecord);
        }
        return hashRecord.concat(probeRecord);
    }

    /**
     * Runs the grace hash join algorithm. Each pass starts by partitioning leftRecords and rightRecords. If we can run
     * build and probe on a partition we should immediately do so, otherwise we should apply the grace hash join
     * algorithm recursively to break up the partitions further.
     */
    private void run(Iterable<Record> leftRecords, Iterable<Record> rightRecords, int pass) {
        assert pass >= 1;
        if (pass > 5) {
            throw new IllegalStateException("Reached the max number of passes");
        }

        // Create empty partitions
        Partition[] leftPartitions = createPartitions(true);
        Partition[] rightPartitions = createPartitions(false);

        // Partition records into left and right
        this.partition(leftPartitions, leftRecords, true, pass);
        this.partition(rightPartitions, rightRecords, false, pass);

        for (int i = 0; i < leftPartitions.length; i++) {
            // TODO(proj3_part1): implement the rest of grace hash join DONE

            if (isReadyForBuildAndProbe(leftPartitions[i], rightPartitions[i])) {
                buildAndProbe(leftPartitions[i], rightPartitions[i]);
            } else {
                run(leftPartitions[i], rightPartitions[i], pass + 1);
            }
        }
    }

    private boolean isReadyForBuildAndProbe(Partition leftPartitions, Partition rightPartitions) {
        return leftPartitions.getNumPages() <= this.numBuffers - 2 ||
                rightPartitions.getNumPages() <= this.numBuffers - 2;
    }

    // Provided Helpers ////////////////////////////////////////////////////////

    /**
     * Create an appropriate number of partitions relative to the number of available buffers we have.
     *
     * @return an array of partitions
     */
    private Partition[] createPartitions(boolean left) {
        int usableBuffers = this.numBuffers - 1;
        Partition partitions[] = new Partition[usableBuffers];
        for (int i = 0; i < usableBuffers; i++) {
            partitions[i] = createPartition(left);
        }
        return partitions;
    }

    /**
     * Creates either a regular partition or a smart partition depending on the value of this.useSmartPartition.
     *
     * @param left true if this partition will store records from the left relation, false otherwise
     * @return a partition to store records from the specified partition
     */
    private Partition createPartition(boolean left) {
        Schema schema = getRightSource().getSchema();
        if (left) {
            schema = getLeftSource().getSchema();
        }
        return new Partition(getTransaction(), schema);
    }

    // Student Input Methods ///////////////////////////////////////////////////

    /**
     * Creates a record using val as the value for a single column of type int. An extra column containing a 500 byte
     * string is appended so that each page will hold exactly 8 records.
     *
     * @param val value the field will take
     * @return a record
     */
    private static Record createRecord(int val) {
        String s = new String(new char[500]);
        return new Record(val, s);
    }

    /**
     * This method is called in testBreakSHJButPassGHJ.
     * <p>
     * Come up with two lists of records for leftRecords and rightRecords such that SHJ will error when given those
     * relations, but GHJ will successfully run. createRecord(int val) takes in an integer value and returns a record
     * with that value in the column being joined on.
     * <p>
     * Hints: Both joins will have access to B=6 buffers and each page can fit exactly 8 records.
     *
     * @return Pair of leftRecords and rightRecords
     */
    public static Pair<List<Record>, List<Record>> getBreakSHJInputs() {

        // TODO(proj3_part1): populate leftRecords and rightRecords such that DONE
        ArrayList<Record> records = new ArrayList<>();
        for (int i = 0; i < 136; i++) { // 135까지는 통과 X
            records.add(createRecord(i));
        }

        ArrayList<Record> leftRecords = new ArrayList<>(records);
        ArrayList<Record> rightRecords = new ArrayList<>(records);
        return new Pair<>(leftRecords, rightRecords);
    }

    /**
     * This method is called in testGHJBreak.
     * <p>
     * Come up with two lists of records for leftRecords and rightRecords such that GHJ will error (in our case hit the
     * maximum number of passes). createRecord(int val) takes in an integer value and returns a record with that value
     * in the column being joined on.
     * <p>
     * Hints: Both joins will have access to B=6 buffers and each page can fit exactly 8 records.
     *
     * @return Pair of leftRecords and rightRecords
     */
    public static Pair<List<Record>, List<Record>> getBreakGHJInputs() {
        ArrayList<Record> leftRecords = new ArrayList<>();
        ArrayList<Record> rightRecords = new ArrayList<>();

        // TODO(proj3_part1): populate leftRecords and rightRecords such that GHJ breaks DONE
        // 일반 파티션을 사용할 때 실패하는지를 확인하기 위해 데이터를 겹치게 만들기
        int count = 100;
        while (count-- > 0) {
            leftRecords.add(createRecord(0));
            rightRecords.add(createRecord(0));
        }
        return new Pair<>(leftRecords, rightRecords);
    }
}

