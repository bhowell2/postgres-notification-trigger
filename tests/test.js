const { Client } = require('pg');
const fs = require('fs');
const path = require('path');

async function getNewTestHelper() {

  function Helper() {

    const channelListeners = {};

    this.client = new Client({
                               // user: 'postgres',
                               // host: 'localhost',
                               // database: 'postgres',
                               // password: 'password',
                               // port: 5432,
                             });

    /*
    * Allows to register a listener that is called for EVERY notification,
    * regardless of channel.
    * */
    this.allNotificationsListener = null;

    this.client.on('notification', msg => {
      const listeners = channelListeners[msg.channel];
      if (listeners) {
        for (let i = 0; i < listeners.length; i++) {
          listeners[i](msg);
        }
      }
      if (this.allNotificationsListener) {
        this.allNotificationsListener(msg);
      }
    });


    this.clearAllListeners = () => {
      for (const channelListenersKey in channelListeners) {
        delete channelListeners[channelListenersKey];
      }
    };

    this.clearListenersForChannel = (channelName) => {
      delete channelListeners[channelName];
    };

    this.addChannelListener = async (channelName, listener) => {
      let listeners = channelListeners[channelName];
      if (listeners) {
        listeners.push(listener);
      } else {
        listeners = [listener];
        channelListeners[channelName] = listeners;
        await this.client.query("LISTEN " + channelName);
      }
    };

    this.removeChannelListener = (channelName, listener) => {
      let listeners = channelListeners[channelName];
      if (listeners) {
        const i = listeners.indexOf(listener);
        if (i >= 0) {
          listeners.splice(i, 1);
        }
      }
    };

    this.connect = async () => {
      await this.client.connect();
      await this.client.query('DROP SCHEMA IF EXISTS public cascade');
      await this.client.query("CREATE SCHEMA public");
      const createNotifsSql = fs.readFileSync(path.resolve('../notifs.sql')).toString();
      await this.client.query(createNotifsSql);
      const testDataSql = fs.readFileSync(path.resolve('./testdata.sql')).toString();
      await this.client.query(testDataSql);
    }

    this.close = async () => {
      await this.client.end();
    };
  }

  let h = new Helper();
  await h.connect();
  return h;
}

let helper = null; // set by beforeEach

beforeEach(async () => {
  helper = await getNewTestHelper();
})

afterEach(async () => {
  await helper.close();
  helper = null;
})

async function insertIntoTestNotifs(col1, col2, col3) {
  await helper.client.query(
    "INSERT INTO test_notifs (col1, col2, col3) " +
    "values ($1, $2, $3)", [
      col1 != null ? col1 : null,
      col2 != null ? col2 : null,
      col3 != null ? col3 : null
    ]);
}

async function insertIntoTrgNotifs(tableName, channelName, notifName, columns, events) {
  await helper.client.query(
    "INSERT INTO trg_notifs (table_name, channel_name, notif_name, columns, events) " +
    "values ($1, $2, $3, $4, $5)", [tableName, channelName, notifName, columns, events]);
}

function expectToContainAllFields(msg) {
  expectToContainFields(msg, ["id", "col1", "col2", "col3"]);
}

/**
 * This is an "only" contains. I.e., if more fields than what were
 * provided were returned this will fail.
 */
function expectToContainFields(msg, fields) {
  let counter = 0;
  const data = JSON.parse(msg.payload).data;
  for (let i = 0; i < fields.length; i++) {
    if (data.hasOwnProperty(fields[i])) {
      counter++;
    }
  }
  // ensures that ONLY the specified keys were provided
  let dataKeyCount = 0;
  for (const dataKey in data) {
    dataKeyCount++;
  }
  expect(counter).toBe(fields.length);
  expect(dataKeyCount).toBe(counter);
}

test("Check notification sent to multiple channels for single table.", async () => {

  let totalNotesCounter = 0;
  helper.allNotificationsListener = (msg) => {
    totalNotesCounter++;
  };

  helper.addChannelListener('chan1', (msg) => {
    expectToContainAllFields(msg);
    const payload = JSON.parse(msg.payload);
    expect(payload.name).toBe("note_chan1");
    expect(payload.event).toBe('INSERT');
  });

  helper.addChannelListener('chan2', msg => {
    expectToContainFields(msg, ['id'])
    const payload = JSON.parse(msg.payload);
    expect(payload.name).toBe("note_chan2");
    expect(payload.event).toBe('INSERT');
    expect(payload.data.id).toBe(4);
  });

  await insertIntoTrgNotifs('test_notifs', 'chan1', 'note_chan1', "{__all__}", "{insert,update,delete}");
  await insertIntoTrgNotifs('test_notifs', 'chan2', 'note_chan2', "{id}", "{insert,update,delete}");

  await helper.client.query("INSERT INTO test_notifs (col1) VALUES (99)");

  let {rows} = await helper.client.query("SELECT * FROM test_notifs");
  expect(rows.length).toBe(4);
  expect(totalNotesCounter).toBe(2);
});



describe("Ensure only subscribed event types are sent.", () => {

  test("Insert events.", async () => {
    let counter = 0;
    let id = null;
    helper.addChannelListener("chan1", (msg) => {
      counter++;
      id = JSON.parse(msg.payload).data.id;
    })
    await insertIntoTrgNotifs('test_notifs', 'chan1', 'note_chan1', "{__all__}", "{insert}");

    await insertIntoTestNotifs(33, 3.14, "yes");

    expect(id).toBe(4);

    await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = " + id);

    await helper.client.query("DELETE FROM test_notifs WHERE id = " + id);
    expect(counter).toBe(1);
  });

  test("Update events.", async () => {
    let counter = 0;
    helper.addChannelListener("chan1", (msg) => {
      counter++;
    })
    await insertIntoTrgNotifs('test_notifs', 'chan1', 'note_chan1', "{__all__}", "{update}");

    // id should be 4 here
    await insertIntoTestNotifs(33, 3.14, "yes");
    expect(counter).toBe(0);

    await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 4");
    expect(counter).toBe(1);

    await helper.client.query("DELETE FROM test_notifs WHERE id = 4");
    expect(counter).toBe(1);
  });

  test("Delete events.", async () => {
    let counter = 0;
    helper.addChannelListener("chan1", (msg) => {
      counter++;
    })
    await insertIntoTrgNotifs('test_notifs', 'chan1', 'note_chan1', "{__all__}", "{delete}");
    // id should be 4 here
    await insertIntoTestNotifs(33, 3.14, "yes");
    expect(counter).toBe(0);

    await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 4");
    expect(counter).toBe(0);

    await helper.client.query("DELETE FROM test_notifs WHERE id = 4");
    expect(counter).toBe(1);
  });

  test("Received all events.", async () => {
    let counter = 0;
    let id = null;
    helper.addChannelListener("chan1", (msg) => {
      counter++;
      id = JSON.parse(msg.payload).data.id;
    })
    await insertIntoTrgNotifs('test_notifs', 'chan1', 'note_chan1', "{__all__}", "{insert,delete,update}");
    // id should be 4 here
    await insertIntoTestNotifs(33, 3.14, "yes");
    expect(counter).toBe(1);

    await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 4");
    expect(counter).toBe(2);

    await helper.client.query("DELETE FROM test_notifs WHERE id = 4");
    expect(counter).toBe(3);
  });

  test("Received only insert and update.", async () => {
    let counter = 0;
    let id = null;
    helper.addChannelListener("chan1", (msg) => {
      counter++;
      id = JSON.parse(msg.payload).data.id;
    })
    await insertIntoTrgNotifs('test_notifs', 'chan1', 'note_chan1', "{__all__}", "{insert,update}");
    // id should be 4 here
    await insertIntoTestNotifs(33, 3.14, "yes");
    expect(counter).toBe(1);

    await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 4");
    expect(counter).toBe(2);

    await helper.client.query("DELETE FROM test_notifs WHERE id = 4");
    expect(counter).toBe(2);
  });

  test("Received only insert and delete.", async () => {
    let counter = 0;
    let id = null;
    helper.addChannelListener("chan1", (msg) => {
      counter++;
      id = JSON.parse(msg.payload).data.id;
    })
    await insertIntoTrgNotifs('test_notifs', 'chan1', 'note_chan1', "{__all__}", "{insert,delete}");
    // id should be 4 here
    await insertIntoTestNotifs(33, 3.14, "yes");
    expect(counter).toBe(1);

    await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 4");
    expect(counter).toBe(1);

    await helper.client.query("DELETE FROM test_notifs WHERE id = 4");
    expect(counter).toBe(2);
  });

  test("Received only update and delete.", async () => {
    let counter = 0;
    let id = null;
    helper.addChannelListener("chan1", (msg) => {
      counter++;
      id = JSON.parse(msg.payload).data.id;
    })
    await insertIntoTrgNotifs('test_notifs', 'chan1', 'note_chan1', "{__all__}", "{update,delete}");
    // id should be 4 here
    await insertIntoTestNotifs(33, 3.14, "yes");
    expect(counter).toBe(0);

    await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 4");
    expect(counter).toBe(1);

    await helper.client.query("DELETE FROM test_notifs WHERE id = 4");
    expect(counter).toBe(2);
  });

});

test("Check update to subscribed events works.", async () => {
  let counter = 0;
  helper.addChannelListener("chan1", (msg) => {
    counter++;
  })
  await insertIntoTrgNotifs('test_notifs', 'chan1', 'note_chan1', "{__all__}", "{update,insert,delete}");
  // id should be 4 here
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(1);

  await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 4");
  expect(counter).toBe(2);

  await helper.client.query("DELETE FROM test_notifs WHERE id = 4");
  expect(counter).toBe(3);

  // change to only listen for insert
  await helper.client.query("UPDATE trg_notifs SET events = '{insert}' WHERE id = 1").catch(err => {
    console.log(err);
    throw err;
  });

  // id should be 5 here
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(4);

  await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 5");
  expect(counter).toBe(4);

  await helper.client.query("DELETE FROM test_notifs WHERE id = 5");
  expect(counter).toBe(4);

  // change to listen to both insert and update

  await helper.client.query("UPDATE trg_notifs SET events='{insert,update}' WHERE id = 1");

  // id should be 6
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(5);

  await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 6");
  expect(counter).toBe(6);

  await helper.client.query("DELETE FROM test_notifs WHERE id = 6");
  expect(counter).toBe(6);

  await helper.client.query("UPDATE trg_notifs SET events='{delete}' WHERE id = 1");

  // id should be 7
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(6);

  await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 7");
  expect(counter).toBe(6);

  await helper.client.query("DELETE FROM test_notifs WHERE id = 7");
  expect(counter).toBe(7);

});

test("Check deletion from trg_notifs table stops notifications to channel.", async () => {
  let counter = 0;
  helper.addChannelListener("chan1", (msg) => {
    counter++;
  })
  await insertIntoTrgNotifs('test_notifs', 'chan1', 'note_chan1', "{__all__}", "{update,insert,delete}");
  // id should be 4 here
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(1);

  await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 4");
  expect(counter).toBe(2);

  await helper.client.query("DELETE FROM test_notifs WHERE id = 4");
  expect(counter).toBe(3);

  // change to only listen for insert

  await helper.client.query("DELETE FROM trg_notifs WHERE id = 1");
  // id should be 5 here
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(3);

  await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 5");
  expect(counter).toBe(3);

  await helper.client.query("DELETE FROM test_notifs WHERE id = 5");
  expect(counter).toBe(3);
});


test("Check change of subscribed columns.", async () => {

  let counter = 0;
  const firstListener = (msg) => {
    counter++;
    expectToContainAllFields(msg);
  };
  helper.addChannelListener("chan1", firstListener)
  await insertIntoTrgNotifs('test_notifs', 'chan1', 'note_chan1', "{__all__}", "{update,insert,delete}");
  // id should be 4 here
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(1);

  await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 4");
  expect(counter).toBe(2);

  await helper.client.query("DELETE FROM test_notifs WHERE id = 4");
  expect(counter).toBe(3);

  // for update below
  helper.removeChannelListener("chan1", firstListener);
  const secondListener = (msg) => {
    counter++;
    expectToContainFields(msg, ['id']);
  }
  helper.addChannelListener("chan1", secondListener);
  await helper.client.query("UPDATE trg_notifs SET columns='{id}' WHERE id = 1");

  // should only contain id field now

  // id should be 5 here
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(4);

  await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 5");
  expect(counter).toBe(5);

  await helper.client.query("DELETE FROM test_notifs WHERE id = 5");
  expect(counter).toBe(6);


  helper.removeChannelListener("chan1", secondListener);
  const thirdListener = (msg) => {
    counter++;
    const payload = JSON.parse(msg.payload);
    if (payload.event === "INSERT" || payload.event === "DELETE") {
      expectToContainAllFields(msg);
    } else if (payload.event === "UPDATE") {
      expectToContainFields(msg, ['col1']);
    } else {
      fail("Should never happen...");
    }
  }
  helper.addChannelListener("chan1", thirdListener);
  await helper.client.query("UPDATE trg_notifs SET columns='{__changes__}' WHERE id = 1");

  // id should be 6 here
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(7);

  await helper.client.query("UPDATE test_notifs set col1 = 99 where id = 6");
  expect(counter).toBe(8);

  await helper.client.query("DELETE FROM test_notifs where id = 6");
  expect(counter).toBe(9);
})

test("Check change of channel name changes notifications to channel.", async () => {
  let counter = 0;
  const firstListener = (msg) => {
    counter++;
    expectToContainAllFields(msg);
  };
  helper.addChannelListener("chan1", firstListener)
  await insertIntoTrgNotifs('test_notifs', 'chan1', 'note_chan1', "{__all__}", "{update,insert,delete}");
  // id should be 4 here
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(1);

  await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 4");
  expect(counter).toBe(2);

  await helper.client.query("DELETE FROM test_notifs WHERE id = 4");
  expect(counter).toBe(3);

  // for update below
  const chan2Listener = (msg) => {
    counter++;
    expectToContainAllFields(msg);
    expect(msg.channel).toBe("chan2");
  }
  helper.addChannelListener("chan2", chan2Listener);
  await helper.client.query("UPDATE trg_notifs SET channel_name='chan2' WHERE id = 1");

  // should be 5 now
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(4);

  await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 5");
  expect(counter).toBe(5);

  await helper.client.query("DELETE FROM test_notifs WHERE id = 5");
  expect(counter).toBe(6);

});

test("Check change of table name changes", async () => {

  let counter = 0;
  const firstListener = (msg) => {
    counter++;
    const payload = JSON.parse(msg.payload);
    expect(msg.channel).toBe("chan1");
    expect(payload.table).toBe('test_notifs');
    expectToContainAllFields(msg);
  };
  helper.addChannelListener("chan1", firstListener)
  await insertIntoTrgNotifs('test_notifs', 'chan1', 'note_chan1', "{__all__}", "{update,insert,delete}");
  // id should be 4 here
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(1);

  await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 4");
  expect(counter).toBe(2);

  await helper.client.query("DELETE FROM test_notifs WHERE id = 4");
  expect(counter).toBe(3);

  // for update below
  helper.removeChannelListener("chan1", firstListener);
  const secondListener = (msg) => {
    counter++;
    expectToContainAllFields(msg);
    const payload = JSON.parse(msg.payload);
    expect(msg.channel).toBe("chan1");
    expect(payload.table).toBe('test_notifs_2');
  }
  helper.addChannelListener("chan1", secondListener);
  await helper.client.query("UPDATE trg_notifs SET table_name='test_notifs_2' WHERE id = 1");

  // should be 5 now
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(3);

  await helper.client.query(
    "INSERT INTO test_notifs_2 (col1, col2, col3) " +
    "values ($1, $2, $3)", [3, 3.111, 'yay']);
  expect(counter).toBe(4);

  await helper.client.query("UPDATE test_notifs_2 SET col1 = 44 WHERE id = 4");
  expect(counter).toBe(5);

  await helper.client.query("DELETE FROM test_notifs_2 WHERE id = 4");
  expect(counter).toBe(6);
});

test("Change notif_name", async () => {
  let counter = 0;
  const firstListener = (msg) => {
    counter++;
    const payload = JSON.parse(msg.payload);
    expect(payload.name).toBe('note_chan1');
    expect(msg.channel).toBe("chan1");
    expect(payload.table).toBe('test_notifs');
    expectToContainAllFields(msg);
  };
  helper.addChannelListener("chan1", firstListener)
  await insertIntoTrgNotifs('test_notifs', 'chan1', 'note_chan1', "{__all__}", "{update,insert,delete}");
  // id should be 4 here
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(1);

  await helper.client.query("UPDATE test_notifs SET col1 = 44 WHERE id = 4");
  expect(counter).toBe(2);

  await helper.client.query("DELETE FROM test_notifs WHERE id = 4");
  expect(counter).toBe(3);

  // for update below
  helper.removeChannelListener("chan1", firstListener);
  const secondListener = (msg) => {
    counter++;
    expectToContainAllFields(msg);
    const payload = JSON.parse(msg.payload);
    expect(payload.name).toBe('note_chan1_2');
    expect(msg.channel).toBe("chan1");
    expect(payload.table).toBe('test_notifs');
  }
  helper.addChannelListener("chan1", secondListener);
  await helper.client.query("UPDATE trg_notifs SET notif_name='note_chan1_2' WHERE id = 1");

  // should be 5 now
  await insertIntoTestNotifs(33, 3.14, "yes");
  expect(counter).toBe(4);

})
