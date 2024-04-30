package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class BNLJIterator implements Iterator<Record>{
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more records in the left source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement DONE
            if (!leftSourceIterator.hasNext()) { // 더 이상 레코드가 없으면 아무 작업도 수행하지 않음
                return;
            }

            // backtracking iterator로 설정
            leftBlockIterator = getBlockIterator(leftSourceIterator, getLeftSource().getSchema(), numBuffers - 2);
            leftBlockIterator.markNext(); // 안해주면 reset()에서 error
            leftRecord = leftBlockIterator.next();
        }

        /**
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right source.
         *
         * If there are no more records in the right source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement DONE
            if (!rightSourceIterator.hasNext()) {  // 더 이상 레코드가 없으면 아무 작업도 수행하지 않음
                return;
            }

            // backtracking iterator로 설정
            rightPageIterator = getBlockIterator(rightSourceIterator, getRightSource().getSchema(), 1);
            rightPageIterator.markNext();
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * You may find JoinOperator#compare useful here. (You can call compare
         * function directly from this file, since BNLJOperator is a subclass
         * of JoinOperator).
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement DONE

            Record rightRecord;
            while (true) {
                if (!rightPageIterator.hasNext()) { // 오른쪽 페이지 반복자에 산출할 값이 있는 경우는 바로 통과
                    if (leftBlockIterator.hasNext()) {
                        // 오른쪽 페이지 반복자에는 산출할 값이 없지만 왼쪽 블록 반복자에는 산출된 값이 있는 경우
                        // 즉, 위에서 오른쪽 끝까지 간 후 한 페이지 내에서 왼쪽 위로 이동 가능
                        this.leftRecord = leftBlockIterator.next();
                        rightPageIterator.reset();
                    } else if (rightSourceIterator.hasNext()) {
                        // 오른쪽 페이지와 왼쪽 블록 반복자 모두 산출할 값이 없지만 오른쪽 페이지가 있는 경우
                        // 즉, 한 페이지 내에서 왼쪽 블록의 반복자가 끝날 때까지 이동을 다 한후 오른쪽 페이지로 이동하여 다시 오른쪽으로 계속 이동 가능
                        leftBlockIterator.reset();
                        this.leftRecord = leftBlockIterator.next();
                        fetchNextRightPage();
                    } else if (leftSourceIterator.hasNext()) {
                        // 위의 세가지 경우에 모두 해당되지는 않지만 조인할 레코드가 있는 경우
                        // 즉, 마지막 페이지(제일 오른쪽 페이지) 내에서 왼쪽 위(왼쪽 블록의 반복자)로 이동 가능
                        rightSourceIterator.reset();
                        fetchNextLeftBlock();
                        fetchNextRightPage();
                    } else { //  조인할 레코드가 더 이상 없는 경우
                        return null;
                    }
                }
                rightRecord = rightPageIterator.next();
                if (compare(this.leftRecord, rightRecord) == 0) break;
            }
            return this.leftRecord.concat(rightRecord);
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}
