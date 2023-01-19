const fs = require('fs');
const pg = require("pg");

const postgresConnectionString = 'postgres://postgres:postgres@localhost:5432/messages';
const fileDirectory = "files/topics/";
const truncateTables = true;
const processAttachments = true;
const attachmentOutputDirectory ="/attachments/";

runProcess(postgresConnectionString,
    fileDirectory,
    truncateTables,
    processAttachments,
    attachmentOutputDirectory);

/**
 * Checks the database schema for the existence of the 'messages', 'topics', and 'attachments' tables. If any of these tables do not exist, they will be created.
 * If the tables already exist and the flag "truncateTables" is set to true, the tables will be truncated.
 *
 * @param {Object} pgClient - The PostgreSQL client instance used to access the database.
 * @param {boolean} truncateTables - Flag indicating whether or not to truncate the existing tables.
 * @returns {void}
 */
async function checkDatabaseSchema(pgClient, truncateTables) {
    const messagesQuery = {
        text: 'SELECT EXISTS (\n' +
            '    SELECT FROM \n' +
            '        pg_tables\n' +
            '    WHERE \n' +
            '        schemaname = \'public\' AND \n' +
            '        tablename  = \'messages\'\n' +
            '    );',
    }
    try {
        const res = await pgClient.query(messagesQuery);
        if (res.rows[0].exists) {
            console.log("Messages table exists");
            if (truncateTables) {
                //Truncate the messages table
                console.log("Truncating messages table");
                const truncateMessagesQuery = {
                    text: 'truncate messages ;' +
                        'ALTER SEQUENCE messages_internalmessageid_seq RESTART;\n' +
                        'UPDATE messages SET internalmessageid  = DEFAULT;',
                }

                await pgClient.query(truncateMessagesQuery);
            }
        } else {
            console.log("Messages table does not exist, creating...");
            await createMessagesTable(pgClient);
        }

    } catch (err) {
        console.log(err.stack);
    }

    const topicsQuery = {
        text: 'SELECT EXISTS (\n' +
            '    SELECT FROM \n' +
            '        pg_tables\n' +
            '    WHERE \n' +
            '        schemaname = \'public\' AND \n' +
            '        tablename  = \'topics\'\n' +
            '    );',
    }
    try {
        const res = await pgClient.query(topicsQuery);
        if (res.rows[0].exists) {
            console.log("Topics table exists");
            if (truncateTables) {
                //Truncate the topics table
                console.log("Truncating topics table");
                const truncateTopicsQuery = {
                    text: 'truncate topics ;\n' +
                    'ALTER SEQUENCE topics_internaltopicid_seq RESTART;\n' +
                    'UPDATE topics  SET internaltopicid  = DEFAULT;',
                }
                await pgClient.query(truncateTopicsQuery);
            }
        } else {
            console.log("Topics table does not exist, creating...");
            await createTopicsTable(pgClient);
        }
    } catch (err) {
        console.log(err.stack);
    }

    const attachmentsQuery = {
        text: 'SELECT EXISTS (\n' +
            '    SELECT FROM \n' +
            '        pg_tables\n' +
            '    WHERE \n' +
            '        schemaname = \'public\' AND \n' +
            '        tablename  = \'attachments\'\n' +
            '    );',
    }
    try {
        const res = await pgClient.query(attachmentsQuery);
        if (res.rows[0].exists) {
            console.log("Attachments table exists");
            if (truncateTables) {
                //Truncate the topics table
                console.log("Truncating attachments table");
                const truncateAttachmentsQuery = {
                    text: 'truncate attachments;\n' +
                        'ALTER SEQUENCE attachments_internalattachmentid_seq RESTART;\n' +
                        'UPDATE attachments  SET internalattachmentid  = DEFAULT;',
                }
                await pgClient.query(truncateAttachmentsQuery);
            }
        } else {
            console.log("Attachments table does not exist, creating...");
            await createAttachmentsTable(pgClient);
        }
    } catch (err) {
        console.log(err.stack);
    }
}

/**
 * Creates a table in a PostgreSQL database to store messages
 * @param {object} postgresClient - A client for the PostgreSQL database.
 * @async
 */
async function createMessagesTable(postgresClient) {
    const createMessagesTableQuery = {
        text: 'CREATE TABLE public.messages (\n' +
            '    internalmessageid integer NOT NULL,\n' +
            '    nummessagesintopic integer,\n' +
            '    nextintime integer,\n' +
            '    systemmessage boolean,\n' +
            '    subject character varying(256),\n' +
            '    messagefrom character varying(256),\n' +
            '    authorname character varying(256),\n' +
            '    msgsnippet character varying(10485759),\n' +
            '    msgid integer,\n' +
            '    rawemail character varying(10485759),\n' +
            '    profile character varying(256),\n' +
            '    userid bigint,\n' +
            '    previntime integer,\n' +
            '    contenttrasformed boolean,\n' +
            '    postdate character varying(256),\n' +
            '    nextintopic integer,\n' +
            '    previntopic integer,\n' +
            '    topicid integer\n' +
            ');\n' +
            '\n' +
            'CREATE SEQUENCE public.messages_internalmessageid_seq\n' +
            '    AS integer\n' +
            '    START WITH 1\n' +
            '    INCREMENT BY 1\n' +
            '    NO MINVALUE\n' +
            '    NO MAXVALUE\n' +
            '    CACHE 1;\n' +
            'ALTER SEQUENCE public.messages_internalmessageid_seq OWNED BY public.messages.internalmessageid;\n' +
            'ALTER TABLE ONLY public.messages ALTER COLUMN internalmessageid SET DEFAULT nextval(\'public.messages_internalmessageid_seq\'::regclass);\n' +
            'ALTER TABLE ONLY public.messages ADD CONSTRAINT messages_pkey PRIMARY KEY (internalmessageid);',
    }
    try {
        await postgresClient.query(createMessagesTableQuery);
    } catch (err) {
        console.log(err.stack);
    }
}

/**
 * Creates a table in a PostgreSQL database to store topics
 * @param {object} postgresClient - A client for the PostgreSQL database.
 * @async
 */
async function createTopicsTable(postgresClient) {
    const createTopicsTableQuery = {
        text: 'CREATE TABLE public.topics (\n' +
            '    internaltopicid integer NOT NULL,\n' +
            '    topicid integer NOT NULL,\n' +
            '    subject character varying\n' +
            ');\n' +
            '\n' +
            '\n' +
            'CREATE SEQUENCE public.topics_internaltopicid_seq\n' +
            '    AS integer\n' +
            '    START WITH 1\n' +
            '    INCREMENT BY 1\n' +
            '    NO MINVALUE\n' +
            '    NO MAXVALUE\n' +
            '    CACHE 1;\n' +
            '\n' +
            '\n' +
            'ALTER SEQUENCE public.topics_internaltopicid_seq OWNED BY public.topics.internaltopicid;\n' +
            '\n' +
            'ALTER TABLE ONLY public.topics ALTER COLUMN internaltopicid SET DEFAULT nextval(\'public.topics_internaltopicid_seq\'::regclass);',
    }
    try {
        await postgresClient.query(createTopicsTableQuery);
    } catch (err) {
        console.log(err.stack);
    }
}

/**
 * Creates a table in a PostgreSQL database to store attachments references
 * @param {object} postgresClient - A client for the PostgreSQL database.
 * @async
 */
async function createAttachmentsTable(postgresClient) {
    const createTopicsTableQuery = {
        text: 'CREATE TABLE public.attachments (\n' +
            '    internalattachmentid integer NOT NULL,\n' +
            '    messageid smallint,\n' +
            '    attachmentfilename character varying,\n' +
            '    attachmentinternalname character varying\n' +
            ');\n' +
            '\n' +
            'CREATE SEQUENCE public.attachments_internalattachmentid_seq\n' +
            '    AS integer\n' +
            '    START WITH 1\n' +
            '    INCREMENT BY 1\n' +
            '    NO MINVALUE\n' +
            '    NO MAXVALUE\n' +
            '    CACHE 1;\n' +
            '\n' +
            'ALTER SEQUENCE public.attachments_internalattachmentid_seq OWNED BY public.attachments.internalattachmentid;\n' +
            '\n' +
            '\n' +
            'ALTER TABLE ONLY public.attachments ALTER COLUMN internalattachmentid SET DEFAULT nextval(\'public.attachments_internalattachmentid_seq\'::regclass);\n' +
            '\n' +
            '\n' +
            'ALTER TABLE ONLY public.attachments\n' +
            '    ADD CONSTRAINT attachments_pkey PRIMARY KEY (internalattachmentid);\n',
    }
    try {
        await postgresClient.query(createTopicsTableQuery);
    } catch (err) {
        console.log(err.stack);
    }
}

/**
 Runs a process, connecting to a Postgres database and reading Yahoo Groups exported JSON format topic files from
 a given directory. For each file, messages are read and inserted into the topics and messages tables.
 * @param {string} postgresConnectionString - The connection string to connect to the Postgres database.
 * @param {string} fileDirectory - The directory to read the files from.
 * @param {boolean} truncateTables - Whether or not to truncate the tables in the database.
 * @param {boolean} processAttachments - Whether or not to process attachments.
 * @param {string} newAttachmentDirectory - The directory to save new attachments to.
 */
async function runProcess(postgresConnectionString, fileDirectory, truncateTables, processAttachments, newAttachmentDirectory) {

    //Connect to the Postgres database
    const client = new pg.Client(postgresConnectionString);
    try {
        await client.connect();
    } catch (err) {
        console.log(err.stack)
        return;
    }
    await checkDatabaseSchema(client, truncateTables);

    //For each file in the given directory (fileDirectory) execute the following
    for (const file of fs.readdirSync(fileDirectory)) {

        if(file.startsWith("message_metadata") || file.startsWith("retrieved") || file.startsWith("unretrievable") || !file.endsWith(".json")) {
            continue;
        }
        //Get the current full file path
        let filePath = fileDirectory + file;

        try {
            //Read the current file and parse into a JSON object
            var currentMessageFile = JSON.parse(fs.readFileSync(filePath, 'utf8'));

            //Read each message located in the topic object
            for(let i=0; i<currentMessageFile["messages"].length; i++) {
                //Load the current message into an object
                let currentMessage = currentMessageFile["messages"][i];

                //Get the topicId of the current JSON object
                var topicId = currentMessage["topicId"];

                if(topicId == null) {
                    console.log("TopicId is null");
                    continue;
                }

                //Default topic insert flag to false
                let insertTopic = false;

                //Check if the topic exists in the database
                const getTopicsQuery = 'select topicid from public.topics t where topicid = $1';
                const getTopicsQueryValues = [topicId];
                try {
                    const res = await client.query(getTopicsQuery, getTopicsQueryValues)
                    switch (res.rows.length) {
                        case 0:
                            insertTopic = true;
                            break;
                        case 1:
                            break;
                        default:
                            console.log("Multiple topics for: " + topicId);
                            break;
                    }
                } catch (err) {
                    console.log(err.stack)
                }

                //If the topic does not exist, insert it into the database
                if (insertTopic) {
                    if ((!currentMessage.subject.startsWith("RE:")) && (!currentMessage.subject.startsWith("Re:"))) {
                        const insertTopicQuery = 'INSERT INTO public.topics(topicid, subject) VALUES($1, $2)'
                        const insertTopicQueryValues = [topicId, currentMessage.subject]

                        try {
                            await client.query(insertTopicQuery, insertTopicQueryValues);
                        } catch (err) {
                            console.log(err.stack)
                        }
                    }
                }

                //Insert the message into the database
                const insertMessageQuery = 'INSERT INTO public.messages\n' +
                    '            (nummessagesintopic, nextintime, ' +
                    '                systemmessage, subject, messagefrom, ' +
                    '                authorname, msgsnippet, msgid, rawemail, profile, ' +
                    '                userid, previntime, contenttrasformed, postdate, nextintopic, previntopic, topicid)' +
                    '            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);'
                const insertMessageQueryValues = [currentMessage.numMessagesInTopic,
                    currentMessage.nextInTime,
                    currentMessage.systemMessage,
                    currentMessage.subject,
                    currentMessage.from,
                    currentMessage.authorName,
                    currentMessage.msgSnippet,
                    currentMessage.msgId,
                    currentMessage.messageBody,
                    currentMessage.profile,
                    currentMessage.userId,
                    currentMessage.prevInTime,
                    currentMessage.contentTrasformed,
                    currentMessage.postDate,
                    currentMessage.nextInTopic,
                    currentMessage.prevInTopic,
                    currentMessage.topicId];

                try {
                    await client.query(insertMessageQuery, insertMessageQueryValues);
                } catch (err) {
                    console.log(err.stack)
                }

                if(processAttachments) {
                    if (currentMessage.hasOwnProperty('attachmentsInfo')) {
                        for (let j = 0; j < currentMessage.attachmentsInfo.length; j++) {
                            let filename = currentMessage.attachmentsInfo[j]["filename"];
                            let filename2 = filename.replace(/[\s\uFEFF\xA0]/g, '-')
                            let attachmentDirectory = fileDirectory + currentMessage.msgId + "_attachments/";
                            let attachmentInternalName = currentMessage.attachmentsInfo[j].fileId + "-" + filename2;
                            let fullAttachmentPath = attachmentDirectory + attachmentInternalName;

                            if (fs.existsSync(fullAttachmentPath)) {
                                console.log("Attachment exists for message " + currentMessage.msgId + ": " + fullAttachmentPath);
                                const insertAttachmentQuery = {
                                    text: 'INSERT INTO public.attachments' +
                                        '(messageid, attachmentfilename, attachmentinternalname)' +
                                        'VALUES($1, $2, $3);',
                                    values: [currentMessage.msgId, filename2, attachmentInternalName],
                                }
                                try {
                                    await client.query(insertAttachmentQuery);
                                } catch (err) {
                                    console.log(err.stack);
                                }

                                fs.copyFile(fullAttachmentPath, newAttachmentDirectory+attachmentInternalName, (err) => {
                                    if (err) throw err;
                                    console.log(attachmentInternalName + ' was copied to ' + newAttachmentDirectory);
                                });
                            } else {
                                console.log("Attachment does not exist for message " + currentMessage.msgId + ": " + fullAttachmentPath);
                            }
                        }
                    }
                } else {
                    console.log("Skipping attachments");
                }
            }
        } catch (e) {
            console.log("failed on " + file);
            console.log(e);
        }
    }
    await client.end();
}