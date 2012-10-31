<?php

class ApiTest extends PHPUnit_Framework_TestCase
{
    protected function setUp()
    {
        dl("history_gluon.so");
    }

    public function testCreateInstance()
    {
        $ctx = create_context();
        $this->assertGreaterThan(0, $ctx);
    }
}
