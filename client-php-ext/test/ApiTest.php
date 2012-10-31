<?php

define("HGL_SUCCESS", 0);

define("HISTORY_GLUON_DELETE_TYPE_EQUAL",            0);
define("HISTORY_GLUON_DELETE_TYPE_EQUAL_OR_LESS",    1);
define("HISTORY_GLUON_DELETE_TYPE_LESS",             2);
define("HISTORY_GLUON_DELETE_TYPE_EQUAL_OR_GREATER", 3);
define("HISTORY_GLUON_DELETE_TYPE_GREATER",          4);

class ApiTest extends PHPUnit_Framework_TestCase
{
    private $g_ctx = null;

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
}
