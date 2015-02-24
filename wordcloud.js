var  http = require('http'),
     connect = require('connect');
     serveStatic = require('serve-static');
     pubsub = require('redis').createClient(),
     redis  = require('redis').createClient(),
     fs = require('fs'),
     sys = require('sys');

var subs = {};
var serve = serveStatic('/')

pubsub.subscribe("TweetSentiment");


http.createServer(function(req, res){
    
    if (req.headers.accept && req.headers.accept == 'text/event-stream') {
        if (req.url == '/streams') {
        
        var args1 = [ 'Tweets', '+inf', '-inf'];
          
         redis.zrevrangebyscore(args1, function (err, w) {
                if (err) throw err;
                console.log('ZRANGE:', w);
                sendWords(req, res,w)
            });
        
            
        } else {
          res.writeHead(404);
          res.end();
        }
   }else
   {
      var file = __dirname + req.url;
           if(req.url === '/') {
               file = __dirname + '/animated.html'
           }
           var callback;
           serveFile(file, req, res, callback); 
       
   }
}).listen(8000);


console.log('Server running at http://127.0.0.1:8000/');


pubsub.on('message', function(channel,message) {
  if(subs[channel])
    subs[channel](message);
   
});


function sendWordsStream(req, res,words) {
  
  subs['TweetSentiment'] = function(message) {

      console.log("update");
      
    var args1 = [ 'Tweets', '+inf', '-inf' ];
    
    redis.zrevrangebyscore(args1, function (err, w) {
        if (err) throw err;
        console.log('ZRANGE:', w);
        sendWords(req, res,w)
    });
    
    
      
   };
   
   sendWords(req,res,words);
};


function sendWords(req, res,words) {
 
 console.log('send words');
 
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });
  
   var id = (new Date()).toLocaleTimeString();
  
   var args = [ 'TweetSentiments', '+inf', '-inf','WITHSCORES'];
          
         redis.zrevrangebyscore(args, function (err, sentiments) {
                if (err) throw err;
                console.log('ZRANGE:', w);
                
                res.write('id: ' + id + '\n');
                res.write("data: " + words + '\n\n');
                res.write("sdata: " + sentiments + '\n\n');
   
   });
            
  //console.log(words);
  
};
 

function serveFile(file, req, res, callback) {
    var fs = require('fs')
        , ext = require('path').extname(file)
        , type = ''
        , fileExtensions = {
            'html':'text/html',
            'css':'text/css',
            'js':'text/javascript',
            'json':'application/json',
            'png':'image/png',
            'jpg':'image/jpg',
            'wav':'audio/wav'
        }
    console.log('req    '+req.url)
    for(var i in fileExtensions) {
       if(ext === i) {    
          type = fileExtensions[i]
          break
       }
    }
    fs.exists(file, function(exists) {
       if(exists) {
          res.writeHead(200, { 'Content-Type': type })
          fs.createReadStream(file).pipe(res)
          console.log('served  '+ req.url)
          if(callback !== undefined) callback()
       } else {
          console.log(file,'file dne')
         }  
    })
}