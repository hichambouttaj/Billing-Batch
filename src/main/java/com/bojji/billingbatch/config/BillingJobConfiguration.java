package com.bojji.billingbatch.config;

import com.bojji.billingbatch.domain.BillingData;
import com.bojji.billingbatch.domain.ReportingData;
import com.bojji.billingbatch.exception.PricingException;
import com.bojji.billingbatch.listener.BillingDataSkipListener;
import com.bojji.billingbatch.processor.BillingDataProcessor;
import com.bojji.billingbatch.service.PricingService;
import com.bojji.billingbatch.step.FilePreparationTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
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
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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
            @Qualifier("billingDataTableWriter") ItemWriter<BillingData> billingDataItemWriter,
            BillingDataSkipListener skipListener
    ) {
        return new StepBuilder("fileIngestion", jobRepository)
                .<BillingData, BillingData>chunk(100, transactionManager)
                .reader(billingDataItemReader)
                .writer(billingDataItemWriter)
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(10)
                .listener(skipListener)
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
                .faultTolerant()
                .retry(PricingException.class)
                .retryLimit(100)
                .build();
    }
    @Bean("billingDataFileReader")
    @StepScope
    public FlatFileItemReader<BillingData> billingDataFileReader(
            @Value("#{jobParameters['input.file']}") String inputFile
    ) {
        return new FlatFileItemReaderBuilder<BillingData>()
                .name("billingDataFileReader")
                .resource(new FileSystemResource(inputFile))
                .delimited()
                .delimiter(",")
                .names("dataYear", "dataMonth", "accountId", "phoneNumber", "dataUsage", "callDuration", "smsCount")
                .targetType(BillingData.class)
                .build();
    }
    @Bean("billingDataTableWriter")
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
    @StepScope
    public JdbcCursorItemReader<BillingData> billingDataTableReader(
            DataSource dataSource,
            @Value("#{jobParameters['data.year']}") Integer year,
            @Value("#{jobParameters['data.month']}") Integer month
    ) {
        String readBillingDataQuery = String.format(" select * from billing_data where DATA_YEAR = %d and DATA_MONTH = %d", year, month);

        return new JdbcCursorItemReaderBuilder<BillingData>()
                .name("billingDataTableReader")
                .dataSource(dataSource)
                .sql(readBillingDataQuery)
                .rowMapper(new DataClassRowMapper<>(BillingData.class))
                .build();
    }
    @Bean
    public BillingDataProcessor billingDataProcessor(PricingService pricingService) {
        return new BillingDataProcessor(pricingService);
    }
    @Bean
    @StepScope
    public FlatFileItemWriter<ReportingData> billingDataFileWriter(
            @Value("#{jobParameters['output.file']}") String outputFile
    ) {
        return new FlatFileItemWriterBuilder<ReportingData>()
                .resource(new FileSystemResource(outputFile))
                .name("billingDataFileWriter")
                .delimited()
                .names("billingData.dataYear", "billingData.dataMonth", "billingData.accountId", "billingData.phoneNumber", "billingData.dataUsage", "billingData.callDuration", "billingData.smsCount", "billingTotal")
                .build();
    }
    @Bean
    @StepScope
    public BillingDataSkipListener skipListener(
            @Value("#{jobParameters['skip.file']}") String skippedFile
    ) {
        return new BillingDataSkipListener(skippedFile);
    }
    @Bean
    public PricingService pricingService() {
        return new PricingService();
    }
}
