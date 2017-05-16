var log = require('logger')('advertisement-service:index');
var nconf = require('nconf');
var utils = require('utils');
var Advertisement = require('advertisement');
var mongutils = require('mongutils');
var sanitizer = require('./sanitizer');
var knox = require('knox');
var path = require('path');
var uuid = require('node-uuid');
var formida = require('formida');
var async = require('async');
var sharp = require('sharp');
var MultiPartUpload = require('knox-mpu');

var express = require('express');
var router = express.Router();

module.exports = router;

var paging = {
    start: 0,
    count: 40,
    sort: ''
};

var fields = {
    '*': true
};

var bucket = 'autos.serandives.com';

var s3Client = knox.createClient({
    secure: false,
    key: nconf.get('awsKey'),
    secret: nconf.get('awsSecret'),
    bucket: bucket
});

var cleanUploads = function (success, failed) {

};

var create = function (err, data, success, failed, req, res) {
    log.debug('add callback');
    if (err) {
        log.error(err);
        cleanUploads(success, failed);
        res.status(500).send([{
            code: 500,
            message: 'Internal Server Error'
        }]);
        return;
    }
    var photo;
    var photos = [];
    for (photo in success) {
        if (success.hasOwnProperty(photo) && !failed[photo]) {
            photos.push(photo);
        }
    }
    data.photos = photos;
    Advertisement.create(data, function (err, advertisement) {
        if (err) {
            log.error(err);
            res.status(500).send([{
                code: 500,
                message: 'Internal Server Error'
            }]);
            return;
        }
        res.status(204).end();
    });
};

var upload = function (name, stream, done) {
    var upload = new MultiPartUpload({
        client: s3Client,
        objectName: name,
        headers: {
            'Content-Type': 'image/jpeg',
            'x-amz-acl': 'public-read'
        },
        stream: stream
    });
    upload.on('initiated', function () {
        log.debug('mpu initiated');
    });
    upload.on('uploading', function () {
        log.debug('mpu uploading');
    });
    upload.on('uploaded', function () {
        log.debug('mpu uploaded');
    });
    upload.on('error', function (err) {
        log.debug('mpu error');
        done(err);
    });
    upload.on('completed', function (body) {
        log.debug('mpu complete');
        done(false, name);
    });
};

var save800x450 = function (id, part, done) {
    var name = 'images/800x450/' + id;
    var transformer = sharp()
        .resize(800, 450)
        .crop(sharp.gravity.center)
        .jpeg()
        .on('error', function (err) {
            log.debug(err);
            console.log(err);
            done(err);
        });
    upload(name, part.pipe(transformer), done);
};

var save288x162 = function (id, part, done) {
    var name = 'images/288x162/' + id;
    var transformer = sharp()
        .resize(288, 162)
        .crop(sharp.gravity.center)
        .jpeg()
        .on('error', function (err) {
            log.debug(err);
            console.log(err);
            done(err);
        });
    upload(name, part.pipe(transformer), done);
};

var update = function (old) {
    return function (err, data, success, failed, req, res) {
        log.debug('update callback');
        if (err) {
            log.error(err);
            res.status(500).send([{
                code: 500,
                message: 'Internal Server Error'
            }]);
            return;
        }
        var photo;
        var photos = [];
        for (photo in success) {
            if (success.hasOwnProperty(photo) && !failed[photo]) {
                photos.push(photo);
            }
        }
        photos = data.photos.concat(photos);
        data.photos = photos;

        var id = req.params.id;
        Advertisement.update({
            _id: id
        }, data, function (err, advertisement) {
            if (err) {
                log.error(err);
                res.status(500).send([{
                    code: 500,
                    message: 'Internal Server Error'
                }]);
                return;
            }
            //TODO: handle 404 case
            res.status(204).end();
        });
        old.photos.forEach(function (photo) {
            var index = photos.indexOf(photo);
            if (index !== -1) {
                return;
            }
            //deleting obsolete photos
            s3Client.deleteFile(photo, function (err, res) {
                log.debug('file:%s is deleted', photo);
            });
        });
    };
};

var process = function (req, res, done) {
    var data;
    var success = [];
    var failed = [];
    //queue is started from 1 as next() is called always at form end
    var queue = 1;
    var next = function (err) {
        if (--queue > 0) {
            return;
        }
        done(false, data, success, failed, req, res);
    };
    var form = new formida.IncomingForm();
    form.on('progress', function (rec, exp) {
        log.debug('received >>> %s', rec);
        log.debug('expected >>> %s', exp);
    });
    form.on('field', function (name, value) {
        if (name !== 'data') {
            return;
        }
        log.debug('%s %s', name, value);
        data = JSON.parse(value);
    });
    form.on('file', function (part) {
        log.debug('file field');
        queue++;
        var id = uuid.v4();
        save800x450(id, part, function (err, name) {
            var photos = err ? failed : success;
            photos = photos[id] || (photos[id] = []);
            photos.push(name);
            next(err);
        });
        queue++;
        save288x162(id, part, function (err, name) {
            var photos = err ? failed : success;
            photos = photos[id] || (photos[id] = []);
            photos.push(name);
            next(err);
        });
    });
    form.on('error', function (err) {
        log.debug(err);
        done(err, data, success, failed, req, res);
    });
    form.on('aborted', function () {
        log.debug('request was aborted');
        done(true, data, success, failed, req, res);
    });
    form.on('end', function () {
        log.debug('form end');
        next();
    });
    form.parse(req);
};
/**
 * { "email": "ruchira@serandives.com", "password": "mypassword" }
 */
router.post('/advertisements', function (req, res) {
    process(req, res, create);
});

/**
 * /advertisements/51bfd3bd5a51f1722d000001
 */
router.get('/advertisements/:id', function (req, res) {
    if (!mongutils.objectId(req.params.id)) {
        res.status(404).send([{
            code: 404,
            message: 'Advertisement Not Found'
        }]);
        return;
    }
    Advertisement.findOne({
        _id: req.params.id
    }).exec(function (err, advertisement) {
        if (err) {
            log.error(err);
            res.status(500).send([{
                code: 500,
                message: 'Internal Server Error'
            }]);
            return;
        }
        if (!advertisement) {
            res.status(404).send([{
                code: 404,
                message: 'Advertisement Not Found'
            }]);
            return;
        }
        var name;
        var opts = [];
        Advertisement.populate(advertisement, opts, function (err, advertisement) {
            if (err) {
                res.status(400).send([{
                    code: 400,
                    message: err
                }]);
                return;
            }
            res.send(advertisement);
        });
    });
});

/**
 * /advertisements/51bfd3bd5a51f1722d000001
 */
router.put('/advertisements/:id', function (req, res) {
    var id = req.params.id;
    if (!mongutils.objectId(id)) {
        res.status(404).send([{
            code: 404,
            message: 'Advertisement Not Found'
        }]);
        return;
    }
    Advertisement.findOne({
        _id: id
    }).exec(function (err, advertisement) {
        if (err) {
            log.error(err);
            res.status(500).send([{
                code: 500,
                message: 'Internal Server Error'
            }]);
            return;
        }
        if (!advertisement) {
            res.status(404).send([{
                code: 404,
                message: 'Advertisement Not Found'
            }]);
            return;
        }
        process(req, res, update(advertisement));
    });
});

/**
 * /advertisements?data={}
 */
router.get('/advertisements', function (req, res) {
    var data = req.query.data ? JSON.parse(req.query.data) : {};
    sanitizer.clean(data.query || (data.query = {}));
    utils.merge(data.paging || (data.paging = {}), paging);
    utils.merge(data.fields || (data.fields = {}), fields);
    Advertisement.find(data.query)
        .skip(data.paging.start)
        .limit(data.paging.count)
        .sort(data.paging.sort)
        .exec(function (err, advertisements) {
            if (err) {
                log.error(err);
                res.status(500).send([{
                    code: 500,
                    message: 'Internal Server Error'
                }]);
                return;
            }
            res.send(advertisements);
        });
});

/**
 * /advertisements/51bfd3bd5a51f1722d000001
 */
router.delete('/advertisements/:id', function (req, res) {
    if (!mongutils.objectId(req.params.id)) {
        res.status(404).send([{
            code: 404,
            message: 'Advertisement Not Found'
        }]);
        return;
    }
    Advertisement.remove({
        _id: req.params.id
    }, function (err) {
        if (err) {
            log.error(err);
            res.status(500).send([{
                code: 500,
                message: 'Internal Server Error'
            }]);
            return;
        }
        res.status(204).end();
    });
});