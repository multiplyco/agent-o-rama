package com.rpl.agentorama;

/**
 * Why a node's execution was invalidated.
 *
 * @see AgentNode#getInvalidationCause()
 */
public enum InvalidationCause {
  /**
   * The run is being retried, and a new attempt replaces this one.
   */
  RETRY,

  /**
   * The run was cancelled.
   */
  CANCEL
}
