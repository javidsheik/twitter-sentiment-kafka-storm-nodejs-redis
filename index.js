var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    client = new kafka.Client(),
    producer = new Producer(client);

var tweetsData = require('tweets.json');

console.log("Simulating Tweets Responses..");
  
    producer.on('ready', function () {
         
            console.log("Connected to kafka...");
         
            var tweetSize = tweetsData.length;
            var count = 0;
            
            setInterval(function() {
            
                //Simulate the time interval.
                
                console.log(msg[count].user.name + " tweeted " + msg[count].text);
                
                var payloads = [{
                
                    topic     : 'tweet-sentiments',
                    partition : 0,
                    messages  : ""+ count + "\t" + msg[count].user.name +"\t" + msg[count].text
            
                    ]}
                  
                    producer.send(payloads, function (err, data) {
                         console.log(data);
                    });
                    
                count++;
                
            },500);
            
    });
    