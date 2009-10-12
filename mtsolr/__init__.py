import simplejson
from httplib import HTTPConnection
import urllib
import uuid

class mtsolr  :
    
    def __init__ (self, host, port=8983, endpoint='/solr') :

        self.host = host
        self.port = port
        self.endpoint = endpoint
        
    def add (self, tags) :

        docs = []
        
        for tag in tags :
            id = 'mt-%s' % uuid.uuid4()

            # FIX ME: escaping and shit
            
            docid = tag['documentid']
            ns = tag['namespace']
            pred = tag['predicate']
            value = tag['value']
        
            doc =  """<doc>
            <field name="uuid">%s</field>
            <field name="documentid">%s</field>
            <field name="namespace">%s</field>
            <field name="predicate">%s</field>
            <field name="value">%s</field>
            </doc>""" % (id, docid, ns, pred, value)

            docs.append(doc)
            
        data = "<add>%s</add>" % "".join(docs)

        print data
        
        if not self._update(data) :
            return None
        
        if not self._commit() :
            return None

        return id
    
    def namespaces(self, predicate=None, value=None) :

        q = []

        if predicate :
            q.append('predicate:%s' % predicate)

        if value :
            q.append('value:%s' % value)

        return self.faceted_search('namespace', q)

    def predicates(self, namespace=None, value=None) :

        q = []

        if namespace :
            q.append('namespace:%s' % namespace)

        if value :
            q.append('value:%s' % value)

        return self.faceted_search('predicate', q)

    def values(self, namespace=None, predicate=None) :

        q = []

        if namespace :
            q.append('namespace:%s' % namespace)

        if predicate :
            q.append('predicate:%s' % predicate)

        return self.faceted_search('predicate', q)

    def documents(self, namespace=None, predicate=None, value=None) :

        q = []

        if namespace :
            q.append('namespace:%s' % namespace)

        if predicate :
            q.append('predicate:%s' % predicate)

        if value :
            q.append('value:%s' % value)

        return self.faceted_search('documentid', q)

    def search (self, args) :

        res = self.execute_request('/select', args)
        # FIX ME
        
    def faceted_search (self, facet, q=[], args={}) :

        q.append('uuid:mt-*')

        args['q'] = ' AND '.join(q)
        args['facet.field'] = facet
        args['rows'] = 0;
        args['facet'] = 'true'
        args['facet.limit'] = -1
        args['facet.mincount'] = 1

        res = self._select(args)

        if not res :
            return None

        raw = res['facet_counts']['facet_fields'][facet]
        idx = range(0, len(raw), 2)

        facets = {}
        
        for i in idx :
            facets[ raw[i] ] = raw[i+1]

        return facets

    def _select (self, args) :

        args['wt'] = 'json'
        query = "?%s" % urllib.urlencode(args)

        rsp = self._execute_request('GET', '/select', query)
        
        try :
            json = simplejson.loads(rsp.read())
        except Exception, e:
            print e
            return None

        return json

    def _update (self, data) :

        if self._execute_request('POST', '/update', data, {'Content-type': 'text/xml'}) :
            return True

        return False
    
    def _commit (self) :

        if self._execute_request('POST', '/update', '<commit />', {'Content-type': 'text/xml'}) :
            return True

        return False

    def _execute_request(self, method, path, body=None, headers={}):

        url = self.endpoint + path

        # why do I need to do this...
        
        if method == 'GET' and body :
            url += body
        
        try :
            conn = HTTPConnection(self.host, self.port)
            conn.request(method, url, body, headers)
            response = conn.getresponse()
            
        except Exception, e :
            return None

        if response.status != 200:
            return None
            
        return response

if __name__ == '__main__' :

    mt = mtsolr('localhost')

    ns = mt.documents('upcoming')
    print ns
    
    print mt.add([{'documentid' : 12342323, 'namespace' : 'upcoming', 'predicate' : 'event', 'value' : '99222129'}])    
    
    ns = mt.documents('upcoming')
    print ns
