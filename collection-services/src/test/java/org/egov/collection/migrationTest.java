package org.egov.collection;

import com.google.gson.Gson;
import org.egov.collection.model.Payment;
import org.egov.collection.model.v1.ReceiptRequest_v1;
import org.egov.collection.model.v1.ReceiptResponse_v1;
import org.egov.collection.service.MigrationService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class migrationTest {

    @Autowired
    MigrationService migrationService;



    @Test
    public void name(){
        Gson gson = new Gson();
        ReceiptRequest_v1 object = null;
        try {
            object = gson.fromJson(new FileReader("/home/rohit/Desktop/Receipt.json"), ReceiptRequest_v1.class);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Payment payment = migrationService.migrateReceipt(object);
        try {
            gson.toJson(payment, new FileWriter("home/rohit/Desktop/Payment.json"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /*public void name(){
        System.out.println("h");
    }*/
}
