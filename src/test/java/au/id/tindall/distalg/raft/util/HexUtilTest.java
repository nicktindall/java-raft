package au.id.tindall.distalg.raft.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HexUtilTest {

    @Test
    void willDumpBytesAsHex() {
        assertThat(HexUtil.hexDump(new byte[]{0xA, 0xB, 0x10, 0x15, 0x55}))
                .isEqualTo("0A 0B 10 15 55");
    }

    @Test
    void willDumpBytesAsHexLength() {
        assertThat(HexUtil.hexDump(new byte[]{0xA, 0xB, 0x10, 0x15, 0x55}, 3))
                .isEqualTo("0A 0B 10");
    }

    @Test
    void willDumpBytesAsHexLengthAndOffset() {
        assertThat(HexUtil.hexDump(new byte[]{0xA, 0xB, 0x10, 0x15, 0x55}, 3, 2))
                .isEqualTo("15 55");
    }
}