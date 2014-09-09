package com.hp.neo4jimport.batchinsert;
/**
 * 更新格式化出来的数据格式为：
 * 如节点：
 * 节点文件头为：Node\tRels\tProperty\n ： Node		Rels	Property\n
 * 节点行数据为：1\t2\tTEST\n			        ：    1		 2		   TEST
 * 格式化出的数据为：
 * 	[Node, 1, Rels, 2, Property, TEST]
 * 偏移量为数据文件格式中每个\t后有偏移量个空字符串。
 * @author shenshouer
 *
 */
public class Data {

	/* 数据集合 */
	private final Object[] data;
	/* 偏移量 */
    private final int offset;
    /* 分隔符 */
    private final String delim;
    
    /**
     * 数据
     * @param header 数据格式头
     * @param delim 分隔符
     * @param offset 偏移量
     */
    public Data(String header, String delim, int offset) {
        this.offset = offset;
        this.delim = delim;
        // 节点文件头 格式：Node\tRels\tProperty\n ： Node	Rels	Property\n
        // 关系文件头格式： Start\tEnde\tType\tProperty\n ：Start	Ende	Type	Property\n
        String[] fields = header.split(delim);
        data = new Object[(fields.length - offset) * 2];
//        System.out.println(fields.length+" "+data.length);
        for (int i = 0; i < fields.length - offset; i++) {
            data[i * 2] = fields[i + offset];
        }
    }
    
    /**
     * 更新数据
     * @param line 每行数据
     * @param header 头部
     * @return
     */
    public Object[] update(String line, Object... header) {
    	if (line.endsWith(delim)) line+=" ";
        final String[] values = line.split(delim);
        if (header.length > 0) {
            System.arraycopy(values, 0, header, 0, header.length);
        }
        for (int i = 0; i < values.length - offset; i++) {
            data[i * 2 + 1] = values[i + offset];
        }
        for (int i = 0; i < data.length ; i++) {
        	if ( data[ i ]==null 
        			|| data[ i ].toString().trim().equals("") 
        			|| data[ i ].equals("null")
        			|| data[ i ].equals("NULL")
        			|| data[ i ].equals("\\N")) 
        		data[ i ] = "";
        }
        return data;
    }

}
