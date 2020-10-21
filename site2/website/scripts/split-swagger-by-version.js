const globby = require('globby');
const fs = require('fs');
const CWD = process.cwd();
var path = require('path');
let staticPath = path.resolve(__dirname, '..');
const patterns = [`${CWD}/static/swagger/*/**.json`];

(async () => {
    const jsonFiles = await globby(patterns);
    let restApiVersions = {}
    
    jsonFiles.map(async (filePath) => {
        let data = fs.readFileSync(filePath)
        let jsonData = JSON.parse(data)
        let pulsarPath = filePath.split('/')
        let reverseList = pulsarPath.reverse()
        let fileName = reverseList[0]
        let pulsarVersion = reverseList[1]
        let restApiVersion = jsonData.info.version
        let restApiDir = `${CWD}/static/swagger/${pulsarVersion}/${restApiVersion}`
        
        if (!fs.existsSync(restApiDir)){
            fs.mkdirSync(restApiDir);
        }
        if (restApiVersions.hasOwnProperty(pulsarVersion)) {
            
            let isHasFileFlag = false
            for (var i = 0; i < restApiVersions[pulsarVersion].length; i++) {
                if (restApiVersion === restApiVersions[pulsarVersion][i]['version']) {
                    isHasFileFlag = true
                    restApiVersions[pulsarVersion][i]['fileName'].push(fileName.slice(0, fileName.lastIndexOf('.')))
                }
            }
            if (!isHasFileFlag) {
                restApiVersions[pulsarVersion].push({
                    version: restApiVersion,
                    fileName: [fileName.slice(0, fileName.lastIndexOf('.'))]
                })
            }
        } else {
            restApiVersions[pulsarVersion] = [{
                version: restApiVersion,
                fileName: [
                    fileName.slice(0, fileName.lastIndexOf('.'))
                ]
            }]

        }
        
        fs.writeFile(restApiDir + '/' + fileName, JSON.stringify(jsonData, null, 4),  function(err) {
            if (err) {
                return console.error(err);
            }
        })
    })
    fs.writeFile(`${staticPath}/static/swagger/restApiVersions.json`, JSON.stringify(restApiVersions, null, 4),  function(err) {
        if (err) {
            return console.error(err);
        }
    })
})();
