package com.miraclelinux.historygluon;

interface StorageDriver {
    public StorageDriver createInstance();
    public boolean init();
    public void close();
    public String getName();

    public boolean addData(HistoryData history);

    public HistoryDataSet getData(long itemId, int clock0, int clock1)
      throws HistoryDataSet.TooManyException;

    public HistoryData getDataWithTimestamp(long itemId, int clock, int ns, boolean searchNear) throws HistoryDataSet.TooManyException;

    public HistoryData getDataWithMinimumClock(long itemId)
      throws HistoryDataSet.TooManyException;

   /**
    * Get the minimum value, the maximum value, and the total count
    * in the specified condition.
    *
    * @param itemId  Item ID
    * @param clock0  The start clock in UNIX time. (The value with this clock
    *                is used.)
    * @param clock1  The end clock in UNIX time. (The value with this clock
    *                 is NOT used.)
    * @return An instance of Statistics on success. Or null.
    *         When there is not data with the specified condition, null is
    *         also returned.
    */
    public Statistics getStatistics(long itemId, int clock0, int clock1)
      throws HistoryDataSet.TooManyException;

   /**
    * Delete data.
    *
    * @param id A data ID.
    * @param sec The second to be used to select deleted data.
    * @param ns The nano second to be used to select deleted data.
    * @param way A type of the deletion.
    * @return The number of deleted data.
    */
    public long delete(long id, int sec, int ns, short way);

    public boolean deleteDB();
}
