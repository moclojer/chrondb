package jdk.internal.misc;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

@TargetClass(className = "jdk.internal.misc.ScopedMemoryAccess")
final class ScopedMemoryAccess__Substitutions {

    @Substitute
    private void closeScope0(Object scope) {
        // Implementação vazia substituída para evitar referências nativas
    }
}