package com.hp.neo4jimport.batchinsert;

/**
 * 数据导入报告
 * @author shenshouer
 *
 */
public class Report {

	/* 批量数  */
	private final long batch;
	/* 顿点数 */
    private final long dots;
    /* 计数器 */
    private long count;
    /* total：当前开始时间，time：导入开始时间，batchTime：每个顿点开始时间 */
    private long total = System.currentTimeMillis(), time, batchTime;

    public Report(long batch, int dots) {
        this.batch = batch;
        this.dots = batch / dots;
    }

    /**
     * 重置
     */
    public void reset() {
        count = 0;
        batchTime = time = System.currentTimeMillis();
    }

    /**
     * 总共消耗时间输出
     */
    public void finish() {
        System.out.println((System.currentTimeMillis() - total) / 1000 + " seconds ");
    }

    /**
     * 每个顿点消耗时间输出
     */
    public void dots() {
        if ((++count % dots) != 0) return;
        System.out.print(".");
        if ((count % batch) != 0) return;
        long now = System.currentTimeMillis();
        System.out.println((now - batchTime) + " ms for "+batch);
        batchTime = now;
    }

    /**
     * 导入总共消耗时间输出
     * @param type
     */
    public void finishImport(String type) {
        System.out.println("\nImporting " + count + " " + type + " took " + (System.currentTimeMillis() - time) / 1000 + " seconds ");
    }
}
