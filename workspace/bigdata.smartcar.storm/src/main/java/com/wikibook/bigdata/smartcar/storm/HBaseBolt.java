package com.wikibook.bigdata.smartcar.storm;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;



@SuppressWarnings("serial")
public class HBaseBolt implements IRichBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseBolt.class);

	protected OutputCollector collector;
	protected HTableConnector connector;
	protected TupleTableConfig conf;
	protected boolean autoAck = true;

	public HBaseBolt(TupleTableConfig conf) {
		this.conf = conf;
	}

	/** {@inheritDoc} */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

		try {
			this.connector = new HTableConnector(conf);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		LOGGER.info("Preparing HBaseBolt for table: " + this.conf.getTableName());
	}

	/** {@inheritDoc} */
	@Override
	public void execute(Tuple input) {
		try {
			this.connector.getTable().put(conf.getPutFromTuple(input));
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}

		if (this.autoAck) {
			this.collector.ack(input);
		}
	}

	/** {@inheritDoc} */
	@Override
	public void cleanup() {
		this.connector.close();
	}

	/** {@inheritDoc} */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	/** {@inheritDoc} */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/**
	 * @return the autoAck
	 */
	public boolean isAutoAck() {
		return autoAck;
	}

	/**
	 * @param autoAck
	 *            the autoAck to set
	 */
	public void setAutoAck(boolean autoAck) {
		this.autoAck = autoAck;
	}
}
