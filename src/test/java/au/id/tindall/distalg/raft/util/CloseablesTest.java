package au.id.tindall.distalg.raft.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class CloseablesTest {

    @Mock
    private AutoCloseable closeable;

    @Test
    void willCloseCloseables() throws Exception {
        Closeables.closeQuietly(closeable);
        verify(closeable).close();
    }

    @Test
    void willLogAndNotPropagateExceptions() throws Exception {
        doThrow(IllegalStateException.class).when(closeable).close();
        Closeables.closeQuietly(closeable);
        verify(closeable).close();
    }

    @Test
    void willNotClearInterruptOnInterruptedException() throws Exception {
        doThrow(InterruptedException.class).when(closeable).close();
        Closeables.closeQuietly(closeable);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        verify(closeable).close();
    }

    @Test
    void willRecursivelyCloseCollections() throws Exception {
        Closeables.closeQuietly(List.of(List.of(closeable)));
        verify(closeable).close();
    }

    @Test
    void willNotAbortOnFirstException() throws Exception {
        doThrow(IllegalStateException.class).when(closeable).close();
        Closeables.closeQuietly(List.of(closeable, List.of(closeable, closeable)));
        verify(closeable, times(3)).close();
    }

    @Test
    void willEmptyCollectionsWhenMutable() throws Exception {
        final List<AutoCloseable> closeables = new ArrayList<>(List.of(closeable, closeable, closeable));
        Closeables.closeQuietly(closeables);
        verify(closeable, times(3)).close();
        assertThat(closeables).isEmpty();
    }
}