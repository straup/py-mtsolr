from httplib import HTTPConnection
import simplejson
import urllib
import uuid
import logging

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

            # ugh...
            
            try :
                docid_l = long(tag['documentid'])
            except :
                docid_l = 0

            try :
                value_f = float(tag['value'])
            except :
                value_f = 0.0

            #
            
            doc =  """<doc>
            <field name="uuid">%s</field>
            <field name="documentid">%s</field>
            <field name="documentid_l">%s</field>            
            <field name="namespace">%s</field>
            <field name="predicate">%s</field>
            <field name="value">%s</field>
            <field name="value_f">%s</field>            
            </doc>""" % (id, docid, docid_l, ns, pred, value, value_f)

            docs.append(doc)
            
        data = "<add>%s</add>" % "".join(docs)
        
        if not self._add(data) :
            return None
        
        if not self._commit() :
            return None

        return id

    def delete (self, uuid) :

        if not self._delete(uuid) :
            return False

        if not self._commit() :
            return False

        return True
    
    def purge (self) :

        if not self._purge() :
            return False

        if not self._commit() :
            return False

        return True
        
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

        res = self._select(args)

        if res : 
            return res['response']

        return None
    
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

        if not rsp :
            return None

        raw = rsp.read()
        # print raw
        
        try :
            json = simplejson.loads(raw)
        except Exception, e:
            logging.error('failed to parse JSON: %s' % e)
            return None

        return json

    def _add (self, data) :

        if self._execute_request('POST', '/update', data, {'Content-type': 'text/xml'}) :
            return True

        return False

    def _delete (self, uuid) :

        xml = "<delete><id>%s</id></delete>" % uuid
        
        if self._execute_request('POST', '/update', xml, {'Content-type': 'text/xml'}) :
            return True

        return False

    def _purge (self) :

        xml = "<delete><query>*:*</query></delete>"
        
        if self._execute_request('POST', '/update', xml, {'Content-type': 'text/xml'}) :
            return True

        return False
        
    def _commit (self) :

        if self._execute_request('POST', '/update', '<commit />', {'Content-type': 'text/xml'}) :
            return True

        return False

    # http://wiki.apache.org/solr/SolrAndHTTPCaches
    
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
            logging.error('HTTP request failed: %s' % e)
            return None

        if response.status != 200:
            logging.error('HTTP response not OK: %s' % response.status)            
            logging.error(response.read())
            return None

        return response

if __name__ == '__main__' :

    import time
    docid = int(time.time()) 

    docid = str(docid) + "s"
    
    mt = mtsolr('localhost')

    mt.purge()
    
    ns = mt.documents('upcoming')
    print ns
    
    uuid = mt.add([{'documentid' : 12342323, 'namespace' : 'upcoming', 'predicate' : 'event', 'value' : docid}])    
    print uuid
    
    ns = mt.documents('upcoming')
    print ns

    query = { 'q' : 'predicate:event'}
    print mt.search(query)

    """
    mt.delete(uuid)

    ns = mt.documents('upcoming')
    print ns

    mt.purge()
    
    ns = mt.documents('upcoming')
    print ns
    """
