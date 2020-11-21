# Pool

based on extension parallel

Example
```php
// bootstrap.php - path to file php where your init code (init framework or connect to db..)
$Pool = new \dizard\Pool(2, 'bootstrap.php');
for ($i=0; $i<5; $i++) {
    // uniname - unique name job, if already has in poll job with these name return false
    $Pool->addNameJob('uniname',function($i) {
        echo $i;
        sleep(2);
        return $i;
    }, [time()]);
}
$res = $Pool->waitCompleteAndGetResults();
var_dump($res); // return [1=>1]
for ($i=0; $i<5; $i++) {
    // uniname - unique name job, if already has in poll job with these name return false
    $Pool->addNameJob(uniqid(),function($i) {
        echo $i;
        sleep(2);
        return $i;
    }, [time()]);
}
$res = $Pool->waitCompleteAndGetResults();
var_dump($res); // return [1=>1, 2=>2,3=>3,4=>4,5=>5]
$Pool->shutdown();
```