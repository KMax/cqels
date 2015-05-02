package org.deri.cqels.lang.cqels;

import com.hp.hpl.jena.sparql.lang.SPARQLParserBase;

public class CQELSParserBase extends SPARQLParserBase
    implements CQELSParserConstants
{
	public Duration getDuration(String str){
		return new Duration(str);
	}
}
