package io.nms.client.cli;

import io.nms.messages.Message;

public interface ResultListener {
  void onResult(Message res);
}
