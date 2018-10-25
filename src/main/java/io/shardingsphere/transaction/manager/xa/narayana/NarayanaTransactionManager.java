/*
 * Copyright 2018 Red Hat, Inc, and individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.shardingsphere.transaction.manager.xa.narayana;

import com.arjuna.ats.arjuna.recovery.RecoveryManager;
import com.arjuna.ats.internal.jta.recovery.arjunacore.XARecoveryModule;
import com.arjuna.ats.jbossatx.jta.RecoveryManagerService;
import com.arjuna.ats.jta.common.jtaPropertyManager;
import com.arjuna.ats.jta.recovery.XAResourceRecoveryHelper;
import io.shardingsphere.core.event.transaction.xa.XATransactionEvent;
import io.shardingsphere.core.rule.DataSourceParameter;
import io.shardingsphere.transaction.manager.xa.XATransactionManager;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import javax.sql.XADataSource;
import javax.transaction.UserTransaction;
import java.sql.SQLException;

/**
 * @author zhfeng
 */
@Slf4j
public class NarayanaTransactionManager implements XATransactionManager {
    private static final UserTransaction USER_TRANSACTION_MANAGER = jtaPropertyManager.getJTAEnvironmentBean().getUserTransaction();
    private static final XARecoveryModule xaRecoveryModule =  XARecoveryModule.getRegisteredXARecoveryModule();
    private static final RecoveryManagerService recoveryManagerService = new RecoveryManagerService();


    public NarayanaTransactionManager () {
        RecoveryManager.delayRecoveryManagerThread();
        recoveryManagerService.create();
        recoveryManagerService.start();
    }

    @Override
    public void destroy() {
        try {
            recoveryManagerService.stop();
        } catch (Exception e) {
            log.warn("stop recoveryManagerService failed with " + e);
        }
        recoveryManagerService.destroy();
    }

    public DataSource wrapDataSource(XADataSource dataSource, String dataSourceName, DataSourceParameter dataSourceParameter) throws Exception {
        DataSource ds = new NarayanaDataSource(dataSource);
        xaRecoveryModule.addXAResourceRecoveryHelper(getRecoveryHelper(dataSource, dataSourceParameter));
        return ds;
    }

    private XAResourceRecoveryHelper getRecoveryHelper(XADataSource xaDataSource, DataSourceParameter dataSourceParameter) {
        String username = dataSourceParameter.getUsername();
        String password = dataSourceParameter.getPassword();

        if (username != null && password != null) {
            return new DataSourceXAResourceRecoveryHelper(xaDataSource, username, password);
        } else {
            return new DataSourceXAResourceRecoveryHelper(xaDataSource);
        }

    }

    @Override
    public void begin(XATransactionEvent transactionEvent) throws SQLException {
        try {
            USER_TRANSACTION_MANAGER.begin();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void commit(XATransactionEvent transactionEvent) throws SQLException {
        try {
            USER_TRANSACTION_MANAGER.commit();
        } catch (Exception e) {
            throw new SQLException(e);
        }

    }

    @Override
    public void rollback(XATransactionEvent transactionEvent) throws SQLException {
        try {
            USER_TRANSACTION_MANAGER.rollback();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getStatus() throws SQLException {
        try {
            return USER_TRANSACTION_MANAGER.getStatus();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
