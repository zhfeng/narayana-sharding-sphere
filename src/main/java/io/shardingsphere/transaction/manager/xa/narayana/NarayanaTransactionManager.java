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
import io.shardingsphere.core.constant.DatabaseType;
import io.shardingsphere.core.exception.ShardingException;
import io.shardingsphere.core.rule.DataSourceParameter;
import io.shardingsphere.transaction.core.context.XATransactionContext;
import io.shardingsphere.transaction.spi.xa.XATransactionManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import javax.sql.XADataSource;
import javax.transaction.TransactionManager;

/**
 * @author zhfeng
 */
@Slf4j
public class NarayanaTransactionManager implements XATransactionManager {
    private static final TransactionManager TRANSACTION_MANAGER = jtaPropertyManager.getJTAEnvironmentBean().getTransactionManager();
    private static final XARecoveryModule xaRecoveryModule = XARecoveryModule.getRegisteredXARecoveryModule();
    private static final RecoveryManagerService recoveryManagerService = new RecoveryManagerService();


    public NarayanaTransactionManager() {
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

    @Override
    public DataSource wrapDataSource(DatabaseType databaseType, XADataSource xaDataSource, String s, DataSourceParameter dataSourceParameter) {
        DataSource ds = new NarayanaDataSource(xaDataSource);
        xaRecoveryModule.addXAResourceRecoveryHelper(getRecoveryHelper(xaDataSource, dataSourceParameter));
        return ds;
    }

    @Override
    public TransactionManager getUnderlyingTransactionManager() {
        return TRANSACTION_MANAGER;
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
    @SneakyThrows
    public void begin(XATransactionContext xaTransactionContext) throws ShardingException {
        TRANSACTION_MANAGER.begin();
    }

    @Override
    @SneakyThrows
    public void commit(XATransactionContext xaTransactionContext) throws ShardingException {
        TRANSACTION_MANAGER.commit();
    }

    @Override
    @SneakyThrows
    public void rollback(XATransactionContext xaTransactionContext) throws ShardingException {
        TRANSACTION_MANAGER.rollback();
    }

    @Override
    @SneakyThrows
    public int getStatus() throws ShardingException {
        return TRANSACTION_MANAGER.getStatus();
    }
}
