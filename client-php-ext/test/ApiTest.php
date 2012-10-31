<?php

define("HGL_SUCCESS", 0);

define("HISTORY_GLUON_DELETE_TYPE_EQUAL",            0);
define("HISTORY_GLUON_DELETE_TYPE_EQUAL_OR_LESS",    1);
define("HISTORY_GLUON_DELETE_TYPE_LESS",             2);
define("HISTORY_GLUON_DELETE_TYPE_EQUAL_OR_GREATER", 3);
define("HISTORY_GLUON_DELETE_TYPE_GREATER",          4);

define("HISTORY_GLUON_DATA_KEY_ID",   "id");
define("HISTORY_GLUON_DATA_KEY_SEC",  "sec");
define("HISTORY_GLUON_DATA_KEY_NS",   "ns");
define("HISTORY_GLUON_DATA_KEY_TYPE", "type");
define("HISTORY_GLUON_DATA_KEY_DATA", "data");

define("HISTORY_GLUON_DATA_TYPE_FLOAT",  0);
define("HISTORY_GLUON_DATA_TYPE_STRING", 1);
define("HISTORY_GLUON_DATA_TYPE_UINT",   2);
define("HISTORY_GLUON_DATA_TYPE_BLOB",   3);

define("TEST_ID_UINT",   0x358);
define("TEST_ID_FLOAT",  0x12345678);
define("TEST_ID_STRING", 0x87654321);
define("TEST_ID_BLOB",   0x102030405060);

define("TEST_NUM_SAMPLES", 5);

class ApiTest extends PHPUnit_Framework_TestCase
{
    /* ----------------------------------------------------------------------------------
     * Private Members
     * ------------------------------------------------------------------------------- */
    private $g_ctx = null;

    /* ----------------------------------------------------------------------------------
     * Test Methods
     * ------------------------------------------------------------------------------- */
    protected function setUp() {
        if (!extension_loaded('History Gluon PHP Extension'))
            dl("history_gluon.so");
    }

    protected function tearDown() {
        if ($this->g_ctx != null) {
            history_gluon_free_context($this->g_ctx);
            $this->g_ctx = null;
        }
    }

    public function testCreateContext() {
        $ctx = history_gluon_create_context();
        $this->assertGreaterThan(0, $ctx);
    }

    public function testFreeContext() {
        $this->assertGloblCreateContext();
        history_gluon_free_context($this->g_ctx);
        $this->g_ctx = null;
    }

    public function testAddUint() {
        $id = 0x12345678;
        $sec = 10203040;
        $ns = 200111500;
        $data = 0x87654321;
        $this->assertGloblCreateContext();
        $this->assertDeleteAllDataWithId($id);
        $ret = history_gluon_add_uint($this->g_ctx, $id, $sec, $ns, $data);
        $this->assertEquals(HGL_SUCCESS, $ret);
    }

    public function testRangeQuery() {
    }

    /* ----------------------------------------------------------------------------------
     * Private Methods
     * ------------------------------------------------------------------------------- */
    private function assertGloblCreateContext() {
        $this->g_ctx = history_gluon_create_context();
        $this->assertGreaterThan(0, $this->g_ctx);
    }

    private function assertDeleteAllDataWithId($id) {
        $num_deleted = 0;
        $ret = history_gluon_delete($this->g_ctx, $id, 0, 0,
                                    HISTORY_GLUON_DELETE_TYPE_EQUAL_OR_GREATER,
                                    $num_deleted);
        $this->assertEquals(HGL_SUCCESS, $ret);
    }

    private function assertSamplesUint() {
        $samples = getSamplesUint();
        $num_samples = count($samples);
        for ($i = 0; $i < $num_samples; $i++) {
            $id   = $samples[$i][HISTORY_GLUON_DATA_KEY_ID];
            $sec  = $samples[$i][HISTORY_GLUON_DATA_KEY_SEC];
            $ns   = $samples[$i][HISTORY_GLUON_DATA_KEY_NS];
            $data = $samples[$i][HISTORY_GLUON_DATA_KEY_DATA];
            $ret = history_gluon_add_uint($this->g_ctx, $id, $sec, $ns, $data);
            $this->assertEquals(HGL_SUCCESS, $ret);
        }
    }

    private function getSamplesUint() {
        $samples[0] = createSample(0x12345678, 100200300, 0x43214321);
        $samples[1] = createSample(0x20000000, 000000000, 0x22223333);
        $samples[2] = createSample(0x20000000, 000000003, 0x9a9a8b8b);
        $samples[3] = createSample(0x25010101, 200345003, 0x9a9a8b8b00);
        $samples[4] = createSample(0x353a0000, 879200003, 0x9a9a8b8b1234);
    }

    private function getSample($sec, $ns, $data) {
        $sample[HISTORY_GLUON_DATA_KEY_ID]   = TEST_ID_UINT;
        $sample[HISTORY_GLUON_DATA_KEY_SEC]  = $sec;
        $sample[HISTORY_GLUON_DATA_KEY_NS]   = $ns;
        $sample[HISTORY_GLUON_DATA_KEY_TYPE] = HISTORY_GLUON_DATA_TYPE_UINT;
        $sample[HISTORY_GLUON_DATA_KEY_DATA] = $data;
    }
}
