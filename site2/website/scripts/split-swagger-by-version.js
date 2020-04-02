const globby = require('globby');
const fs = require('fs');
const CWD = process.cwd()

const patterns = [`${CWD}/static/swagger/*/*.json`];

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
            if (restApiVersions[pulsarVersion].indexOf(restApiVersion) < 0) {
                restApiVersions[pulsarVersion].push(restApiVersion)
            }
        } else {
            restApiVersions[pulsarVersion] = [restApiVersion]
        }
        fs.writeFile(restApiDir + '/' + fileName, JSON.stringify(jsonData, null, 4),  function(err) {
            if (err) {
                return console.error(err);
            }
        })
    })
    fs.writeFile(`${CWD}/static/swagger/restApiVersions.json`, JSON.stringify(restApiVersions, null, 4),  function(err) {
        if (err) {
            return console.error(err);
        }
    })
})();
