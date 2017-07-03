<?php
$xml = simplexml_load_file("final_evaluation_corpus_only-en.v2.snt.pretty.xml");

$filename = 1;

foreach ($xml->record as $record) {
    $xmlWriter = new XMLWriter();
    $xmlWriter->openURI("output/" . $filename . ".xliff");
    $xmlWriter->setIndent(true);
    $xmlWriter->startDocument("1.0", "UTF-8");
    $xmlWriter->startElement("xliff");
    $xmlWriter->writeAttribute("version", "1.0");
    $xmlWriter->startElement("file");
    $xmlWriter->writeAttribute("original", $record->attributes()->id);
    $xmlWriter->writeAttribute("source-language", "en");
    foreach ($record->titles->title as $title) {
        if ($title->attributes()->lang == "en") {
            $xmlWriter->startElement("trans-unit");
            $xmlWriter->writeAttribute("id", uniqid("clubs-",true));
            $xmlWriter->writeElement("source", strval($title));
            $xmlWriter->endElement();
        }
    }
    foreach ($record->abstracts->abstract as $abstract) {
        foreach ($abstract->sentence as $sentence) {
            $xmlWriter->startElement("trans-unit");
            $xmlWriter->writeAttribute("id", uniqid("clubs-",true));
            $xmlWriter->writeElement("source", strval($sentence));
            $xmlWriter->endElement();
        }
    }
    $xmlWriter->endElement();//file
    $xmlWriter->endElement();//xliff
    $filename++;
}