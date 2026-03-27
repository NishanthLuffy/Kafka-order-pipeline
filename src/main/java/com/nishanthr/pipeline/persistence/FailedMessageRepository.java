package com.nishanthr.pipeline.persistence;

import com.nishanthr.pipeline.model.FailedMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface FailedMessageRepository extends JpaRepository<FailedMessage, Long> {

    List<FailedMessage> findByStatus(FailedMessage.Status status);

    List<FailedMessage> findByOrderId(String orderId);

    @Query("SELECT f FROM FailedMessage f WHERE f.status = 'FAILED' ORDER BY f.failedAt ASC")
    List<FailedMessage> findAllPendingRedrive();

    boolean existsByEventId(String eventId);
}