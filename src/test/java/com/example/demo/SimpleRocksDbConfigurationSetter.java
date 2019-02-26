package com.example.demo;

import java.util.Map;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.CompactionOptionsFIFO;
import org.rocksdb.CompactionStyle;
import org.rocksdb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleRocksDbConfigurationSetter implements RocksDBConfigSetter {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleRocksDbConfigurationSetter.class);
  private static final long MAX_DB_SIZE = 52428800; // 50 mb
  private static final int MAX_OPEN_FILES = 500;

  public SimpleRocksDbConfigurationSetter() {
  }

  @Override
  public void setConfig(String storeName, Options options, Map<String, Object> configs) {
    LOG.info("Setting max table file size to [{}] for store with name [{}].", MAX_DB_SIZE, storeName);
    options.setCompactionStyle(CompactionStyle.FIFO);
    CompactionOptionsFIFO compactionOptionsFIFO = new CompactionOptionsFIFO();
    compactionOptionsFIFO.setMaxTableFilesSize(MAX_DB_SIZE);
    options.setCompactionOptionsFIFO(compactionOptionsFIFO);
    LOG.info("Setting max open files to [{}] for store with name [{}].", MAX_OPEN_FILES, storeName);
    options.setMaxOpenFiles(MAX_OPEN_FILES);
  }
}
