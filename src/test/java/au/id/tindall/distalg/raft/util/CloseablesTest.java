package au.id.tindall.distalg.raft.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
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

    @Test
    void willCloseMaps() {
        CloseableThing k1 = new CloseableThing();
        CloseableThing v1 = new CloseableThing();
        CloseableThing k2 = new CloseableThing();
        CloseableThing v2 = new CloseableThing();
        HashMap<CloseableThing, CloseableThing> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        Closeables.closeQuietly(map);
        assertThat(k1.closed).isTrue();
        assertThat(v1.closed).isTrue();
        assertThat(k2.closed).isTrue();
        assertThat(v2.closed).isTrue();
        assertThat(map).isEmpty();
    }

    static class CloseableThing implements Closeable {
        private boolean closed = false;

        @Override
        public void close() {
            closed = true;
        }
    }
}