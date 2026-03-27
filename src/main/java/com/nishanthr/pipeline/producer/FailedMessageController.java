package com.nishanthr.pipeline.producer;

import com.nishanthr.pipeline.consumer.RedriveService;
import com.nishanthr.pipeline.model.FailedMessage;
import com.nishanthr.pipeline.persistence.FailedMessageRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/failed-messages")
public class FailedMessageController {

    private final FailedMessageRepository failedMessageRepository;
    private final RedriveService redriveService;

    public FailedMessageController(FailedMessageRepository failedMessageRepository,
                                    RedriveService redriveService) {
        this.failedMessageRepository = failedMessageRepository;
        this.redriveService = redriveService;
    }

    @GetMapping
    public ResponseEntity<List<FailedMessage>> getAllFailed() {
        return ResponseEntity.ok(
                failedMessageRepository.findByStatus(FailedMessage.Status.FAILED));
    }

    @GetMapping("/all")
    public ResponseEntity<List<FailedMessage>> getAll() {
        return ResponseEntity.ok(failedMessageRepository.findAll());
    }

    @GetMapping("/{id}")
    public ResponseEntity<FailedMessage> getById(@PathVariable Long id) {
        return failedMessageRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/redrive")
    public ResponseEntity<RedriveService.RedriveResult> redriveById(@PathVariable Long id) {
        RedriveService.RedriveResult result = redriveService.redriveById(id);
        return result.success()
                ? ResponseEntity.ok(result)
                : ResponseEntity.internalServerError().body(result);
    }

    @PostMapping("/redrive-all")
    public ResponseEntity<RedriveService.RedriveSummary> redriveAll() {
        return ResponseEntity.ok(redriveService.redriveAll());
    }

    @PostMapping("/{id}/resolve")
    public ResponseEntity<Map<String, String>> manuallyResolve(
            @PathVariable Long id,
            @RequestBody(required = false) Map<String, String> body) {

        return failedMessageRepository.findById(id).map(fm -> {
            fm.setStatus(FailedMessage.Status.RESOLVED);
            fm.setResolutionNotes(body != null ? body.get("notes") : "Manually resolved");
            failedMessageRepository.save(fm);
            return ResponseEntity.ok(Map.of(
                    "status", "RESOLVED",
                    "id", id.toString(),
                    "notes", fm.getResolutionNotes()
            ));
        }).orElse(ResponseEntity.notFound().build());
    }
}