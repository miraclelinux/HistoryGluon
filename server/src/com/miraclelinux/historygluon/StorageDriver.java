package com.miraclelinux.historygluon;

interface StorageDriver {
    public StorageDriver createInstance();
    public boolean init();
    public void close();
    public String getName();
    public void setDatabase(String dbName);

    public int addData(HistoryData history);

    public HistoryDataSet getData(long itemId, int clock0, int ns0, int clock1, int ns1)
      throws HistoryDataSet.TooManyException;

    public HistoryData queryData(long itemId, int clock, int ns,
                                 int searchWhenNotFound)
      throws HistoryDataSet.TooManyException;

    public HistoryData getMinimumTime(long itemId)
      throws HistoryDataSet.TooManyException;

   /**
    * Get the minimum value, the maximum value, and the total count
    * in the specified condition.
    *
    * @param itemId  Item ID
    * @param sec0  The start time in UNIX time. (The value with this clock is used.)
    * @param ns0  The start time of nanosecond.
    * @param se1  The end time in UNIX time. (The value with this clock is NOT used.)
    * @param ns1  The end time of nanosecond.
    * @return An instance of Statistics on success. null on errors.
    *         When the data with the specified condition doesn't exist,
    *         null is also returned.
    */
    public Statistics getStatistics(long itemId, int sec0, int ns0, int sec1, int ns1)
      throws HistoryDataSet.TooManyException, HistoryData.DataNotNumericException;

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
