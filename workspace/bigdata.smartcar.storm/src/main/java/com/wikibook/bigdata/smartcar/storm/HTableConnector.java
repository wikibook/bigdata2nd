package com.wikibook.bigdata.smartcar.storm;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class HTableConnector implements Serializable {

	private static final Logger LOGGER = LoggerFactory.getLogger(HTableConnector.class);

	private Configuration config;
	protected HTable table;
	private String tableName;

	public HTableConnector(final TupleTableConfig conf) throws IOException {
		this.tableName = conf.getTableName();
		this.config = HBaseConfiguration.create();

		this.config.set("hbase.zookeeper.quorum", conf.getZkQuorum());
		this.config.set("hbase.zookeeper.property.clientPort",
				conf.getZkClientPort());
		this.config.set("hbase.cluster.distributed", "true");

		LOGGER.info(String.format("Initializing connection to HBase table %s at %s", tableName,
				this.config.get("hbase.rootdir")));

		try {
			this.table = new HTable(this.config, this.tableName);
		} catch (IOException ex) {
			throw new IOException(
					"Unable to establish connection to HBase table "
							+ this.tableName, ex);
		}

		if (conf.isBatch()) {
			// Enable client-side write buffer
			this.table.setAutoFlush(false, true);
			LOGGER.info("Enabled client-side write buffer");
		}

		// If set, override write buffer size
		if (conf.getWriteBufferSize() > 0) {
			try {
				this.table.setWriteBufferSize(conf.getWriteBufferSize());

				LOGGER.info("Setting client-side write buffer to "
						+ conf.getWriteBufferSize());
			} catch (IOException ex) {
				LOGGER.error(
						"Unable to set client-side write buffer size for HBase table "
								+ this.tableName, ex);
			}
		}

		// Check the configured column families exist
		for (String cf : conf.getColumnFamilies()) {
			if (!columnFamilyExists(cf)) {
				throw new RuntimeException(String.format("HBase table '%s' does not have column family '%s'", conf.getTableName(), cf));
			}
		}
	}

	private boolean columnFamilyExists(final String columnFamily)
			throws IOException {
		return this.table.getTableDescriptor().hasFamily(Bytes.toBytes(columnFamily));
	}

	/**
	 * @return the table
	 */
	public HTable getTable() {
		return table;
	}

	/**
	 * Close the table
	 */
	public void close() {
		try {
			this.table.close();
		} catch (IOException ex) {
			LOGGER.error("Unable to close connection to HBase table " + tableName, ex);
		}
	}
}