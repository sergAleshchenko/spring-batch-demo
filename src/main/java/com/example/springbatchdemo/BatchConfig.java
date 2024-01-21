package com.example.springbatchdemo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.math.BigDecimal;

/**
 * @author Sergei Aleshchenko
 */

@Configuration
@EnableBatchProcessing //(1)
public class BatchConfig {
    private static final Log log = LogFactory.getLog(BatchConfig.class);
    @Bean // (2)
    public Tasklet clearTableTasklet(JdbcTemplate jdbcTemplate) {
        return (stepContribution, chunkContext) -> {
            log.info("Очистка таблицы sales_report");
            jdbcTemplate.update("delete from sales_report");
            return RepeatStatus.FINISHED;
        };
    }

    @Bean //(3)
    public Step setupStep(Tasklet clearTableTasklet,
                          StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("clear-report-table")
                .tasklet(clearTableTasklet)
                .build();
    }

    @Bean // (4)
    public Step loadCsvStep(StepBuilderFactory stepBuilderFactory,
                            FlatFileItemReader<SalesReportItem> csvReader,
                            ItemProcessor<SalesReportItem, SalesReportItem> totalCalculatingProcessor,
                            JdbcBatchItemWriter<SalesReportItem> dbWriter) {
        return stepBuilderFactory.get("load-csv-file")
                .<SalesReportItem, SalesReportItem>chunk(10) // (5)
                .faultTolerant()
                .skip(IncorrectValueException.class) // (6)
                .skipLimit(3) // (7)
                .reader(csvReader) // (8)
                .processor(totalCalculatingProcessor) // (9)
                .writer(dbWriter) // (10)
                .build();
    }
    @Bean
    @StepScope // (11)
    public FlatFileItemReader<SalesReportItem> csvReader() {
        return new FlatFileItemReaderBuilder<SalesReportItem>().name("csv-reader")
                .resource(new ClassPathResource("report_data.csv"))
                .targetType(SalesReportItem.class)
                .delimited()
                .delimiter("|")
                .names("regionId", "outletId", "smartphones", "memoryCards", "notebooks").build();
    }
    @Bean // (12)
    public ItemProcessor<SalesReportItem, SalesReportItem> totalCalculatingProcessor() {
        return item -> {
            if (BigDecimal.ZERO.compareTo(item.getSmartphones()) > 0
                    || BigDecimal.ZERO.compareTo(item.getMemoryCards()) > 0
                    || BigDecimal.ZERO.compareTo(item.getNotebooks()) > 0) {
                throw new IncorrectValueException();
            }
            item.setTotal(BigDecimal.ZERO.add(item.getSmartphones())
                    .add(item.getMemoryCards()
                            .add(item.getNotebooks())));
            return item;
        };
    }
    @Bean // (13)
    public JdbcBatchItemWriter<SalesReportItem> dbWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<SalesReportItem>()
                .dataSource(dataSource)
                .sql("insert into sales_report (region_id, outlet_id, smartphones, memory_cards, notebooks, total) " +
                        "values (:regionId, :outletId, :smartphones, :memoryCards, :notebooks, :total)")
                .beanMapped()
                .build();
    }
    @Bean // (14)
    public Job importReportJob(JobBuilderFactory jobBuilderFactory,
                               Step setupStep,
                               Step loadCsvStep,
                               ReportImportListener reportImportListener) {
        return jobBuilderFactory.get("import-report-job")
                .incrementer(new RunIdIncrementer())
                .listener(reportImportListener) // (15)
                .start(setupStep)
                .next(loadCsvStep)
                .build();
    }

}
