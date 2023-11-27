package com.bojji.billingbatch.config;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.repository.JobRepository;

@RequiredArgsConstructor
public class BillingJob implements Job {
    private final JobRepository jobRepository;
    @Override
    public String getName() {
        return "BillingJob";
    }
    @Override
    public void execute(JobExecution execution) {
        System.out.println("processing billing information");
        execution.setStatus(BatchStatus.COMPLETED);
        execution.setExitStatus(ExitStatus.COMPLETED);
        jobRepository.update(execution);
    }
}
