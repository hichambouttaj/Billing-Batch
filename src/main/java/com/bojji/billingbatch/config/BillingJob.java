package com.bojji.billingbatch.config;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.*;
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
        JobParameters jobParameters = execution.getJobParameters();
        String inputFile = jobParameters.getString("input.file");
        System.out.println("processing billing information from file " + inputFile);
        execution.setStatus(BatchStatus.COMPLETED);
        execution.setExitStatus(ExitStatus.COMPLETED);
        jobRepository.update(execution);
    }
}
