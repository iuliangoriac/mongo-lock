monitor_tailable = (blockId) => {
    while (true) {
        var currentOps = db.getSiblingDB("admin").aggregate([
                { $currentOp: { allUsers: false } },
                { $match: { "cursor.originatingCommand.filter.id": blockId } }
            ]).toArray();
        print(currentOps.length)
    }
}

debug_tailable = () => {
    while(true) {
        var currentOps = db.getSiblingDB("admin").aggregate([{ $currentOp: { allUsers: false } }]).toArray();
        var blocks = currentOps.filter((doc) => doc.cursor);
        if (blocks.length) {
            print(blocks.length);
        } else {
            print(currentOps);
        }
    }
}

debug_tailable_no_aggregation = () => {
    while(true) {
        var currentOps = db.currentOp({ $ownOps: true }).inprog;
        var blocks = currentOps.filter((doc) => doc.cursor);
        if (blocks.length) {
            print(blocks.length);
        } else {
            print(currentOps);
        }
    }
}