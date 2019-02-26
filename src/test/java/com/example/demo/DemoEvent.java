package com.example.demo;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import java.util.UUID;

public class DemoEvent {
  private String name;
  private String body;
  private Long timestamp;
  @JsonSerialize(using = ToStringSerializer.class)
  private UUID uuid;

  public DemoEvent() {
  }

  public DemoEvent(String name, String body, Long timestamp, UUID uuid) {
    this.name = name;
    this.body = body;
    this.timestamp = timestamp;
    this.uuid = uuid;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getBody() {
    return body;
  }

  public void setBody(String body) {
    this.body = body;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public UUID getUuid() {
    return uuid;
  }

  public void setUuid(UUID uuid) {
    this.uuid = uuid;
  }
}
