package com.bojji.billingbatch.config;

import com.bojji.billingbatch.domain.BillingData;
import com.bojji.billingbatch.domain.ReportingData;
import com.bojji.billingbatch.processor.BillingDataProcessor;
import com.bojji.billingbatch.step.FilePreparationTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BillingJobConfiguration {
    @Bean
    public Job job(JobRepository jobRepository, Step filePreparationStep, Step fileIngestionStep, Step reportGenerationStep) {
        return new JobBuilder("BillingJob", jobRepository)
                .start(filePreparationStep)
                .next(fileIngestionStep)
                .next(reportGenerationStep)
                .build();
    }
    @Bean
    public Step filePreparationStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("filePreparation", jobRepository)
                .tasklet(new FilePreparationTasklet(), transactionManager)
                .build();
    }
    @Bean
    public Step fileIngestionStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("billingDataFileReader") ItemReader<BillingData> billingDataItemReader,
            ItemWriter<BillingData> billingDataItemWriter) {
        return new StepBuilder("fileIngestion", jobRepository)
                .<BillingData, BillingData>chunk(100, transactionManager)
                .reader(billingDataItemReader)
                .writer(billingDataItemWriter)
                .build();
    }
    @Bean
    public Step reportGenerationStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("billingDataTableReader") ItemReader<BillingData> billingDataTableReader,
            ItemProcessor<BillingData, ReportingData> billingDataProcessor,
            ItemWriter<ReportingData> billingDataFileWriter
    ) {
        return new StepBuilder("reportGeneration", jobRepository)
                .<BillingData, ReportingData>chunk(100, transactionManager)
                .reader(billingDataTableReader)
                .processor(billingDataProcessor)
                .writer(billingDataFileWriter)
                .build();
    }
    @Bean("billingDataFileReader")
    public FlatFileItemReader<BillingData> billingDataFileReader() {
        return new FlatFileItemReaderBuilder<BillingData>()
                .name("billingDataFileReader")
                .resource(new FileSystemResource("staging/billing-2023-01.csv"))
                .delimited()
                .delimiter(",")
                .names("dataYear", "dataMonth", "accountId", "phoneNumber", "dataUsage", "callDuration", "smsCount")
                .targetType(BillingData.class)
                .build();
    }
    @Bean
    public JdbcBatchItemWriter<BillingData> billingDataTableWriter(DataSource dataSource) {
        String insertBillingDataQuery = """
                insert into billing_data values (:dataYear, :dataMonth, :accountId, :phoneNumber, :dataUsage, :callDuration, :smsCount)
                """;

        return new JdbcBatchItemWriterBuilder<BillingData>()
                .dataSource(dataSource)
                .sql(insertBillingDataQuery)
                .beanMapped()
                .build();
    }
    @Bean("billingDataTableReader")
    public JdbcCursorItemReader<BillingData> billingDataTableReader(DataSource dataSource) {
        String readBillingDataQuery = """
                select * from billing_data
                """;

        return new JdbcCursorItemReaderBuilder<BillingData>()
                .name("billingDataTableReader")
                .dataSource(dataSource)
                .sql(readBillingDataQuery)
                .rowMapper(new DataClassRowMapper<>(BillingData.class))
                .build();
    }
    @Bean
    public BillingDataProcessor billingDataProcessor() {
        return new BillingDataProcessor();
    }
    @Bean
    public FlatFileItemWriter<ReportingData> billingDataFileWriter() {
        return new FlatFileItemWriterBuilder<ReportingData>()
                .resource(new FileSystemResource("staging/billing-report-2023-01.csv"))
                .name("billingDataFileWriter")
                .delimited()
                .names("billingData.dataYear", "billingData.dataMonth", "billingData.accountId", "billingData.phoneNumber", "billingData.dataUsage", "billingData.callDuration", "billingData.smsCount", "billingTotal")
                .build();
    }
}
