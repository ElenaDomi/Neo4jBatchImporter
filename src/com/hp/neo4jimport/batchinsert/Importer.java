package com.hp.neo4jimport.batchinsert;

import static org.neo4j.helpers.collection.MapUtil.map;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.neo4j.index.impl.lucene.LuceneIndexImplementation;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.LuceneBatchInserterIndexProvider;

/**
 * 批量数据导入工具
 * 
 * 
 */
public class Importer {

	Logger log = Logger.getLogger(Importer.class);
	private static final String configPath = Importer.class.getResource("/").getPath()
			+ "batchImporter.properties";
	private final String NODE_INDEX_NAME = "nodes";
//	private final String ENTPRIPID = "node";
	public static final String S001 = "\u0001";

	/* 导入报告 */
	private static Report report;
	/* 批量导入工具 */
	private BatchInserter db;
	/* 批量索引建立工具 */
	private BatchInserterIndexProvider indexProvider;
	/* 节点索引工具 */
	private BatchInserterIndex nodeIndex;

	private Configuration conf;
	
	private String[] floatRel;
	
	private String[] floatNode;
	
	public static final Map<String, String> config = getConfig();

	public Importer(File graphDb) {
		db = BatchInserters.inserter(graphDb.getAbsolutePath(), config);
		conf = new Configuration();
		indexProvider = new LuceneBatchInserterIndexProvider(db);
		nodeIndex = indexProvider.nodeIndex(NODE_INDEX_NAME,
				LuceneIndexImplementation.EXACT_CONFIG);
		report = new Report(10 * 1000 * 1000, 100);
		floatRel = config.get("floatRel").split("-");
		floatNode = config.get("floatNode").split("-");
	}

	public static void main(String[] args) throws IOException {
		File graphDb = new File(config.get("graphPath"));
		if (!graphDb.exists())
			graphDb.mkdirs();
		else {
			FileUtils.deleteRecursively(graphDb);
		}
		Importer importBatch = new Importer(graphDb);
		String nodesPath = config.get("prePath") + config.get("nodesFile");
		String relationshipsPath = config.get("prePath") + config.get("relationshipsFile");
		try {
			importBatch.importNodes(nodesPath);
			importBatch.importRelationships(relationshipsPath);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			importBatch.finish();
		}
	}

	private void importNodes(String path) throws Exception {
		report.reset();
		String nodeHead=config.get("nodeHead");
		nodeHead=nodeHead.replaceAll("-", S001);
		final Data data = new Data(nodeHead, S001, 0);
		FileSystem hdfs = FileSystem.get(URI.create(path), conf);
		FileStatus fileList[] = hdfs.listStatus(new Path(path.toLowerCase()));
		for (FileStatus fi : fileList) {
			if (!fi.isDirectory()) {
				insertNode(fi.getPath(), data);
			}
		}
		report.finishImport("Nodes");
	}

	private void importRelationships(String path) throws Exception {
		nodeIndex.flush();
		String relaHead=config.get("relaHead");
		relaHead=relaHead.replaceAll("-", S001);
		final Data data = new Data(relaHead, S001, 3);
		report.reset();
		FileSystem hdfs = FileSystem.get(URI.create(path), conf);
		FileStatus fileList[] = hdfs.listStatus(new Path(path.toLowerCase()));
		for (FileStatus fi : fileList) {
			if (!fi.isDirectory()) {
				insertRela(fi.getPath(), data);
			}
		}
		report.finishImport("Relationships");
	}

	/**
	 * 获取配置文件
	 * 
	 * @return
	 */
	public static Map<String, String> getConfig() {
		Map<String, String> config = new HashMap<String, String>();
		Properties pro = new Properties();
		try {
			pro.load(new FileReader(new File(configPath)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (Object str : pro.keySet()) {
			if (str instanceof String && pro.get(str) instanceof String) {
				config.put((String) str, (String) pro.get(str));
			}
		}
		return config;
	}

	private long id(Object id) {
		return Long.parseLong((String) id);
	}

	private void finish() {
		indexProvider.shutdown();
		db.shutdown();
		report.finish();
	}

	private void insertNode(Path path, Data data) throws IOException {
		FileSystem hdfs = FileSystem.get(path.toUri(), conf);
		FSDataInputStream dis = hdfs.open(path);
		BufferedReader in = new BufferedReader(new InputStreamReader(dis),
				1024 * 1024 * 10);
		String value = null;
		while ((value = in.readLine()) != null) {// 逐行读取
			int pos = value.indexOf(S001);
			long nodeId = Long.parseLong(value.substring(0,pos));
			value = value.substring(pos+1);
			Map<String, Object> properties = map(data.update(value));
			for (String key : floatNode) {
				String tmp = properties.get(key).toString();
				if (StringUtils.isEmpty(tmp)) tmp = "0";
				float tmpFloat;
				try {
					tmpFloat = Float.parseFloat(tmp);
				} catch (NumberFormatException e) {
					log.error(tmp+" is not float");
					tmpFloat = 0;
				}
				properties.put(key, tmpFloat);
			}
			db.createNode(nodeId, properties);
			report.dots();
			nodeIndex.add(nodeId,properties);
		}
		in.close();
		dis.close();
	}

	private void insertRela(Path path, Data data) throws Exception {
		FileSystem hdfs = FileSystem.get(path.toUri(), conf);
		FSDataInputStream dis = hdfs.open(path);
		BufferedReader in = new BufferedReader(new InputStreamReader(dis),
				1024 * 1024 * 10);
		Object[] rel = new Object[3];
		final RelType type = new RelType();
		String value = null;
		while ((value = in.readLine()) != null) {// 逐行读取
			Map<String, Object> properties = map(data.update(value, rel));
			for (String key : floatRel) {
				String tmp = properties.get(key).toString();
				if (StringUtils.isEmpty(tmp)) tmp = "0";
				float tmpFloat;
				try {
					tmpFloat = Float.parseFloat(tmp);
				} catch (NumberFormatException e) {
					log.error(tmp+" is not float");
					tmpFloat = 0;
				}
				properties.put(key, tmpFloat);
			}
			long nodeA = id(rel[0]);
			long nodeB = id(rel[1]);
			if ((nodeA > 0) && (nodeB > 0)) {
				db.createRelationship(nodeA, nodeB,type.update(rel[2]), properties);
				report.dots();
			}
		}
	}

}
