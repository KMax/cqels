package org.deri.cqels.lang.cqels;

import java.io.Reader ;
import java.io.StringReader ;

import com.hp.hpl.jena.query.Query ;
import com.hp.hpl.jena.query.QueryException ;
import com.hp.hpl.jena.query.QueryParseException ;
import com.hp.hpl.jena.query.Syntax ;
import com.hp.hpl.jena.shared.JenaException ;
import com.hp.hpl.jena.sparql.lang.Parser;
import com.hp.hpl.jena.sparql.lang.ParserSPARQL11;
import com.hp.hpl.jena.sparql.syntax.Element ;
import com.hp.hpl.jena.sparql.syntax.Template ;
import org.openjena.atlas.logging.Log ;


public class ParserCQELS extends Parser
{
	public static final Syntax syntaxCQELS_01    = Syntax.make("http://deri.org/2011/05/query/CQELS_01") ;
	  
	private interface Action { void exec(CQELSParser parser) throws Exception ; }
    
    @Override
    protected Query parse$(final Query query, String queryString)
    {
        query.setSyntax(syntaxCQELS_01) ;

        Action action = new Action() {
            public void exec(CQELSParser parser) throws Exception
            {
                parser.QueryUnit() ;
            }
        } ;

        perform(query, queryString, action) ;
        validateParsedQuery(query) ;
        return query ;
    }
    
    public static Element parseElement(String string)
    {
        final Query query = new Query () ;
        Action action = new Action() {
            public void exec(CQELSParser parser) throws Exception
            {
                Element el = parser.GroupGraphPattern() ;
                query.setQueryPattern(el) ;
            }
        } ;
        perform(query, string, action) ;
        return query.getQueryPattern() ;
    }
    
    public static Template parseTemplate(String string)
    {
        final Query query = new Query () ;
        Action action = new Action() {
            public void exec(CQELSParser parser) throws Exception
            {
                Template t = parser.ConstructTemplate() ;
                query.setConstructTemplate(t) ;
            }
        } ;
        perform(query, string, action) ;
        return query.getConstructTemplate() ;
    }
    
 
    // All throwable handling.
    private static void perform(Query query, String string, Action action)
    {
        Reader in = new StringReader(string) ;
        CQELSParser parser = new CQELSParser(in) ;

        try {
            query.setStrict(true) ;
            parser.setQuery(query) ;
            action.exec(parser) ;
        }
        catch (com.hp.hpl.jena.sparql.lang.arq.ParseException ex)
        { 
            throw new QueryParseException(ex.getMessage(),
                                          ex.currentToken.beginLine,
                                          ex.currentToken.beginColumn
                                          ) ; }
        catch (com.hp.hpl.jena.sparql.lang.arq.TokenMgrError tErr)
        {
            // Last valid token : not the same as token error message - but this should not happen
            int col = parser.token.endColumn ;
            int line = parser.token.endLine ;
            throw new QueryParseException(tErr.getMessage(), line, col) ; }
        
        catch (QueryException ex) { throw ex ; }
        catch (JenaException ex)  { throw new QueryException(ex.getMessage(), ex) ; }
        catch (Error err)
        {
            // The token stream can throw errors.
            throw new QueryParseException(err.getMessage(), err, -1, -1) ;
        }
        catch (Throwable th)
        {
            Log.warn(ParserSPARQL11.class, "Unexpected throwable: ",th) ;
            throw new QueryException(th.getMessage(), th) ;
        }
    }
}
/*
 * (c) Copyright 2005, 2006, 2007, 2008, 2009 Hewlett-Packard Development Company, LP
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */