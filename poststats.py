function extractSkills() {
    let dataJson = {};

    /* These are the JSON keys we are interested in */
    const KEYS = ['skillLevel', 'skillXP', 'username']

    /* Loop over the save file JSON keys */
    for (let i = 0; i < allVars.length; i++) {
        /* Reached a key we are interested in */
        if (KEYS.includes(allVars[i])) {
            /* Add to our JSON */
            dataJson[allVars[i]] = getItem(allVars[i]);
        }
    }

    /* gzip and B64 encode */
    const pakoSave = pako.gzip(JSON.stringify(dataJson), { to: 'string' });
    return [ dataJson['username'], btoa(pakoSave) ];
}

function sendToHiscoresAPI(username, b64JsonString) {
    $.ajax({
        url: 'https://l9ahyalvt7.execute-api.us-east-1.amazonaws.com/prod/users/jadedtdt',
        type: 'POST',
        async: true,
        data: JSON.stringify({
            "data" : "test"
        }),
        success: function(data) {
            console.log('Updated hiscores for user: jadedtdt');
        }
    });
}

function main() {
    let [ username, data ] = extractSkills();
    sendToHiscoresAPI('jadedtdt', 'melvor test');
}
main();
