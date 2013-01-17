package com.teradata.qaf.tset.run;

import com.teradata.qaf.tset.utils.XMLReader;

public class XmlReaderRun {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		XMLReader xmlReader = new XMLReader("ConfFile_schema.xml");
		xmlReader.parseXml();
	}

}
