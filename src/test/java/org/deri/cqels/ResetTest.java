package org.deri.cqels;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import org.deri.cqels.engine.ExecContext;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

public class ResetTest {

    private static final String CQELS_HOME = "cqels_home";

    @Test
    @Ignore //Fails
    public void test() {
        final File HOME = new File(CQELS_HOME);
        if(!HOME.exists()) {
            HOME.mkdir();
        }
        final ExecContext context = new ExecContext(CQELS_HOME, true);
        
        context.env().close();
        context.getDataset().close();
        context.getARQExCtx().getDataset().close();
        context.dictionary().close();

        try {
            Files.walkFileTree(new File(CQELS_HOME).toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }

            });
        } catch (IOException ex) {
            fail(ex.getMessage());
        }
    }
}