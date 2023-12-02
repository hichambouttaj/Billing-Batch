package com.bojji.billingbatch.listener;

import com.bojji.billingbatch.domain.BillingData;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.item.file.FlatFileParseException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class BillingDataSkipListener implements SkipListener<BillingData, BillingData> {
    private Path skippedItemsFile;
    public BillingDataSkipListener(String skippedItemsFile) {
        this.skippedItemsFile = Paths.get(skippedItemsFile);
    }
    @Override
    public void onSkipInRead(Throwable throwable) {
        if(throwable instanceof FlatFileParseException flatFileParseException) {
            String rawLine = flatFileParseException.getInput();
            int lineNumber = flatFileParseException.getLineNumber();
            String skippedLine = lineNumber + "|" + rawLine + System.lineSeparator();
            try {
                Files.writeString(this.skippedItemsFile, skippedLine, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            }catch (IOException exception) {
                throw new RuntimeException("Unable to write skipped item " + skippedLine);
            }
        }
    }
}
